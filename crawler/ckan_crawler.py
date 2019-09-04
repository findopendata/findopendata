import os
import time

import requests
import urllib3
import dateutil.parser
import dateutil.tz
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from celery.utils.log import get_task_logger

from .celery import app
from .storage import save_file, save_object
from .download import download_to_local
from .util import temporary_directory, get_safe_filename
from .zip import is_zipfile, unzip
from .settings import crawler_configs, db_configs, gcp_configs
from .parsers.csv import csv2json
from .parsers.avro import JSON2AvroRecords


# Working directory for tasks.
working_dir = crawler_configs.get("working_dir", "tmp")

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
            default, packages with modified time before the previously
            registered time will be skipped.
    """
    package_id = package["id"]

    # Initialize Postgres connection.
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor()

    # Checks if this version of package has been processed.
    if not force_update:
        cur.execute("SELECT modified::timestamptz "
                "FROM findopendata.ckan_packages "
                "WHERE endpoint = %s AND package_id = %s;",
                (endpoint, package_id))
        row = cur.fetchone()
        if row is not None:
            last_registered = row[0]
            last_updated = _extract_timestamp_from_package(package)
            if last_updated is not None and last_updated <= last_registered:
                logger.info("(endpoint={} package={}) Skipping (updated: {}, "
                        "registered: {})".format(endpoint, package_id,
                            last_updated, last_registered))
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
            "SET modified = current_timestamp, "
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
            "ON CONFLICT (package_key, resource_id, filename) "
            "DO UPDATE "
            "SET modified = current_timestamp, "
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
    cur.execute("SELECT modified::timestamptz FROM findopendata.ckan_resources "
            "WHERE package_key = %s AND resource_id = %s LIMIT 1;",
            (package_key, resource_id))
    row = cur.fetchone()
    if row is not None:
        last_registered = row[0]
        last_updated = _extract_timestamp_from_resource(resource)
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
    with temporary_directory(working_dir) as parent_dir:

        # Download this resource.
        try:
            filename = download_to_local(original_url, parent_dir)
        except Exception as e:
            logger.warning("(package={} resource={}) Failed to download {}: "
                    "{}".format(package_key, resource_id, original_url, e))
            return
        filenames = [filename,]

        # Unzip.
        if is_zipfile(os.path.join(parent_dir, filename)):
            filenames = unzip(os.path.join(parent_dir, filename), parent_dir)

        # Save and register each file.
        for filename in filenames:
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
                continue
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
                    "ON CONFLICT (package_key, resource_id, filename) "
                    "DO UPDATE "
                    "SET modified = current_timestamp, "
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
            default, packages with modified time before the previously
            registered time will be skipped.
    """
    logger.info("(api_url={})".format(api_url))
    packages = read_ckan_api(api_url)
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


def read_ckan_api(api_url, start=0, page_size=50, retries=3,
        wait_between_retries=5):
    """Scrolls through the CKAN package_search API to obtain packages.

    Args:
        api_url: the CKAN API endpoint URL (i.e., https://data.gov.uk).
        start: the starting record index.
        page_size: the number of records per each request.
        retries: the number of retries when an error is encountered.
        wait_between_retries: the seconds to wait between retries.
    """
    url = api_url.rstrip("/") + "/api/3/action/package_search"
    sess = requests.session()
    while True:
        resp = sess.get(url, params={"start" : start,
                                     "rows"  : page_size})
        try:
            resp.raise_for_status()
        except Exception as e:
            if retries == 0:
                raise e
            retries -= 1
            time.sleep(wait_between_retries)
            continue
        results = resp.json()["result"]["results"]
        if len(results) == 0:
            break
        for package in results:
            yield package
        start += page_size


def _parse_ckan_timestamp(timestamp_str):
    try:
        timestamp = dateutil.parser.parse(timestamp_str)
        # Set tzinfo to UTC when not available
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=dateutil.tz.tzutc())
    except ValueError:
        return None
    return timestamp


def _extract_timestamp_from_package(package):
    package_modified = None
    if "modified" in package:
        package_modified = package["modified"]
    elif "metadata_modified" in package:
        package_modified = package["metadata_modified"]
    elif "metadata_created" in package:
        package_modified = package["metadata_created"]
    if package_modified is None:
        return None
    return _parse_ckan_timestamp(package_modified)


def _extract_timestamp_from_resource(resource):
    resource_modified = None
    if "created" in resource and resource["created"] is not None:
        resource_modified = resource["created"]
    if "revision_timestamp" in resource and \
            resource["revision_timestamp"] is not None:
        resource_modified = resource["revision_timestamp"]
    if "last_modified" in resource and resource["last_modified"] is not None:
        resource_modified = resource["last_modified"]
    if resource_modified is None:
        return None
    return _parse_ckan_timestamp(resource_modified)

