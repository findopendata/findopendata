import os
import time

import requests
import urllib3
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from celery.utils.log import get_task_logger

from .celery import app
from .ckan import read_api, extract_timestamp_from_package, \
    extract_timestamp_from_resource
from .storage import save_file, save_object
from .download import download_to_local
from .util import temporary_directory, get_safe_filename
from .zip import is_zipfile, unzip
from .settings import crawler_configs, db_configs, gcp_configs
from .parsers.csv import csv2json
from .parsers.avro import JSON2AvroRecords


# Logger for tasks.
logger = get_task_logger(__name__)

# Dataset formats for which we have available parsers.
accepted_resource_formats = ["csv",]


@app.task(ignore_result=True)
def add_ckan_package(package, endpoint, bucket_name, blob_prefix,
        force_update):
    """Registers the CKAN package and starts tasks for retrieving and adding
    associated resources (i.e. files).

    Args:
        package: the CKAN package JSON.
        endpoint: the CKAN endpoint (i.e., data.gov.uk) without http://.
        bucket_name: the name of the cloud storage bucket to upload all blobs.
        blob_prefix: the prefix for all the blobs uploaded from this
            function, relative to the root of the bucket.
        force_update: whether to force update packages in the registry. By
            default, packages with updated time before the previously
            registered time will be skipped.
    """
    package_id = package["id"]

    # Initialize Postgres connection.
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor()

    # Checks if this version of package has been processed.
    if not force_update:
        cur.execute("SELECT updated::timestamptz "
                "FROM findopendata.ckan_packages "
                "WHERE endpoint = %s AND package_id = %s;",
                (endpoint, package_id))
        row = cur.fetchone()
        if row is not None:
            last_registered = row[0]
            last_updated = extract_timestamp_from_package(package)
            if last_updated is not None and last_updated <= last_registered:
                logger.info("(endpoint={} package={}) Skipping (updated: {}, "
                        "registered: {})".format(endpoint, package_id,
                            last_updated, last_registered))
                cur.close()
                conn.close()
                return

    # Upload the package JSON.
    package_blob = save_object(package, bucket_name,
            os.path.join(blob_prefix, endpoint, package_id, "package.json"))
    logger.info("(endpoint={} package={}) Saved package JSON.".format(endpoint,
        package_id))

    # Register the package.
    cur.execute("INSERT INTO findopendata.ckan_packages "
            "(endpoint, package_id, package_blob) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (endpoint, package_id) DO UPDATE "
            "SET updated = current_timestamp, "
            "package_blob = EXCLUDED.package_blob RETURNING key;",
            (endpoint, package_id, package_blob.name))
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("(endpoint={} package={}) Failed to fetch key".format(
            endpoint, package_id))
    package_key = row[0]
    logger.info("(endpoint={} package={}) Registered package.".format(endpoint,
        package_id))

    # Commit and close database connection.
    conn.commit()
    cur.close()
    conn.close()

    # Skip processing resources if none exists.
    if "resources" not in package:
        return

    resource_blob_prefix = os.path.join(blob_prefix, endpoint, package_id)
    # Start tasks for processing resources.
    for resource in package["resources"]:
        if "format" not in resource or resource["format"].strip().lower() \
                not in accepted_resource_formats:
            add_ckan_resource_no_download(package_key=package_key,
                    resource=resource)
        else:
            add_ckan_resource.delay(package_key=package_key, resource=resource,
                    bucket_name=bucket_name, package_path=resource_blob_prefix)


@app.task(ignore_result=True)
def add_ckan_resource_no_download(package_key, resource):
    """Register this CKAN resource without downloading the associated files.

    Args:
        package_key: the key of the package associated with the resource.
        resource: the portion of the JSON data in the CKAN package
            corresponds to the resource.
    """
    # Get attributes of this resource.
    resource_id = resource.get("id", None)
    if resource_id is None:
        raise ValueError("(package={}) no resource id found.".format(
            package_key))
    original_url = resource.get("url", None)
    if original_url is None:
        logger.warning("(package={}, resource={}) no url found".format(
            package_key, resource_id))
        return
    filename = None

    # Initialize Postgres connection
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor()

    # Save this resource.
    cur.execute("INSERT INTO findopendata.ckan_resources "
            "(package_key, resource_id, filename, original_url, raw_metadata) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (package_key, resource_id) "
            "DO UPDATE "
            "SET updated = current_timestamp, "
            "filename = EXCLUDED.filename, "
            "original_url = EXCLUDED.original_url, "
            "raw_metadata = EXCLUDED.raw_metadata;",
            (package_key, resource_id, filename, original_url, Json(resource)))
    
    # Save and close database connection
    conn.commit()
    cur.close()
    conn.close()

    # Done
    logger.info("(package={}, resource={}, filename={}) Registered "
            "resource.".format(package_key, resource_id, filename))


@app.task(ignore_result=True)
def add_ckan_resource(package_key, resource, bucket_name, package_path):
    """Retrieves and adds CKAN resource files for the given resource.

    Args:
        package_key: the key of the package associated with the resource.
        resource: the portion of the JSON data in the CKAN package
            corresponds to the resource.
        bucket_name: the name of the cloud storage bucket to upload all blobs.
        package_path: the storage bucket path to the directory corresponding
            to the package associated with the resource.
    """
    resource_id = resource.get("id", None)
    if resource_id is None:
        raise ValueError("(package={}) no resource id found.".format(
            package_key))
    original_url = resource.get("url", None)
    if original_url is None:
        logger.warning("(package={}, resource={}) no url found".format(
            package_key, resource_id))
        return

    # Initialize Postgres connection for checking resource.
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor()

    # Check if the same version of this resource has been processed.
    cur.execute("SELECT updated::timestamptz FROM findopendata.ckan_resources "
            "WHERE package_key = %s AND resource_id = %s;",
            (package_key, resource_id))
    row = cur.fetchone()
    if row is not None:
        last_registered = row[0]
        last_updated = extract_timestamp_from_resource(resource)
        if last_updated is not None and last_updated <= last_registered:
            logger.info("(package={}, resource={}) Skipping (updated: {} "
                    "registered: {})".format(package_key, resource_id,
                        last_updated, last_registered))
            return

    # Close database connection for checking resource.
    cur.close()
    conn.close()

    # Download and upload the resource.
    logger.info("(package={} resource={}) Saving resource from {}".format(
        package_key, resource_id, original_url))
    working_dir = crawler_configs.get("working_dir", "/tmp")
    with temporary_directory(working_dir) as parent_dir:

        # Download this resource.
        try:
            filename = download_to_local(original_url, parent_dir)
        except Exception as e:
            logger.warning("(package={} resource={}) Failed to download {}: "
                    "{}".format(package_key, resource_id, original_url, e))
            return

        # Save and register the resource file.
        # Build blob name
        blob_name = os.path.join(package_path, resource_id, filename)
        try:
            with open(os.path.join(parent_dir, filename), "rb") as f:
                resource_blob = save_file(f, bucket_name, blob_name,
                        guess_content_bytes=1024*10)
        except Exception as e:
            logger.warning("(package={} resource={}) Failed to save local "
                    "file {} to {}: {}".format(package_key, resource_id,
                        filename, blob_name, e))
            return
        logger.info("(package={} resource={} filename={}) Saved resource "
                "from {} to {}".format(package_key, resource_id, filename,
                    original_url, blob_name))

    # Initialize Postgres connection for registering resource.
    # A new connection is created here to prevent the download
    # from hogging the connection pool.
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor()

    # Register this resource.
    cur.execute("INSERT INTO findopendata.ckan_resources "
            "(package_key, resource_id, filename, resource_blob, "
            "original_url, file_size, raw_metadata) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (package_key, resource_id) "
            "DO UPDATE "
            "SET updated = current_timestamp, "
            "resource_blob = EXCLUDED.resource_blob, "
            "original_url = EXCLUDED.original_url, "
            "file_size = EXCLUDED.file_size, "
            "raw_metadata = EXCLUDED.raw_metadata;",
            (package_key, resource_id, filename,
                resource_blob.name, original_url,
                resource_blob.size, Json(resource)))
    conn.commit()

    # Close database connection for registering resource.
    cur.close()
    conn.close()

    logger.info("(package={}, resource={}, filename={}) Registered "
            "resource.".format(package_key, resource_id, filename))


@app.task(ignore_result=True)
def add_ckan_packages_from_api(api_url, endpoint, bucket_name, blob_prefix,
        force_update):
    """Scrolls through the CKAN package_search API to obtain packages and
    starts tasks to retrieve and add those packages.

    Args:
        api_url: the CKAN API endpoint URL (i.e., https://data.gov.uk).
        endpoint: the CKAN API endpoint without scheme (i.e., data.gov.uk).
        bucket_name: the name of the cloud storage bucket to upload all blobs.
        blob_prefix: the prefix for all the blobs uploaded from this
            function, relative to the root of the bucket.
        force_update: whether to force update packages in the registry. By
            default, packages with updated time before the previously
            registered time will be skipped.
    """
    logger.info("(api_url={})".format(api_url))
    packages = read_api(api_url)
    for package in packages:
        add_ckan_package.delay(package, endpoint=endpoint, 
                bucket_name=bucket_name, blob_prefix=blob_prefix, 
                force_update=force_update)


@app.task(ignore_result=True)
def add_ckan_apis(force_update):
    """Add CKAN API endpoints to the crawler."""
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor()
    cur.execute(r"""SELECT scheme, endpoint 
                    FROM findopendata.ckan_apis 
                    WHERE enabled = true""")
    def _get_api_url(scheme, endpoint):
        endpoint = endpoint.rstrip("/")
        return ("{}://{}".format(scheme, endpoint), endpoint)
    api_urls = [_get_api_url(scheme, endpoint) for scheme, endpoint in cur]
    cur.close()
    conn.close()
    for api_url, endpoint in api_urls:
        add_ckan_packages_from_api.delay(api_url=api_url,
                endpoint=endpoint,
                bucket_name=gcp_configs.get("bucket_name"),
                blob_prefix=crawler_configs.get("ckan_blob_prefix"),
                force_update=force_update)
        logger.info("Adding CKAN API {} to the crawler".format(api_url))
