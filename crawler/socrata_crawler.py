import re
import io

import requests
import dateutil.parser
import psycopg2
from celery.utils.log import get_task_logger

from .celery import app
from .storage import save_avro_records, save_object
from .parsers.avro import JSON2AvroRecords
from .settings import crawler_configs, db_configs, gcp_configs
from .socrata import socrata_records


# Logger for tasks.
logger = get_task_logger(__name__)


@app.task(ignore_result=True)
def add_socrata_resource(metadata, app_token, bucket_name, blob_prefix,
        force_update):
    """Retrieves and adds a Socrata resource to the registry.

    Args:
        metadata: the 'resource' section of the JSON data returned from
            the Socrata Discovery API.
        app_token: the application token for using the discovery API.
        bucket_name: the name of the cloud storage bucket to upload to.
        blob_prefix: the prefix for all the blobs uploaded from this
            function, relative to the root of the bucket.
        force_update: whether to force update regardless of the updated time.
    """

    domain = metadata["metadata"]["domain"]
    try:
        uid, original_url = _extract_socrata_uid2(metadata)
    except Exception as e:
        logger.warning("(domain={} Failed to extract UID and resource URL from "
                "{}: {}".format(domain, metadata["permalink"], e))
        return

    if not force_update:
        # Initialize Postgres connection and cursor.
        conn = psycopg2.connect(**db_configs)
        cur = conn.cursor()

        # Check if the resource already exists and is updated.
        cur.execute("SELECT updated::timestamptz "
                "FROM findopendata.socrata_resources "
                "WHERE domain = %s AND id = %s;", (domain, uid))
        row = cur.fetchone()
        if row is not None:
            last_registered = row[0]
            last_updated = dateutil.parser.parse(
                    metadata["resource"]["updatedAt"])
            if last_updated <= last_registered:
                logger.info("(domain={} id={}) Skipping (updated {}, "
                        "registered {})".format(domain, uid, last_updated,
                            last_registered))
                return

        # Close the database connection to prevent download hogging the pool.
        cur.close()
        conn.close()

    # Upload the metadata.
    metadata_blob = save_object(metadata, bucket_name,
            "/".join([blob_prefix, domain, uid, "metadata.json"]))
    logger.info("(domain={} id={}) Saved metadata.".format(domain, uid))

    # Download and upload the resource.
    logger.info("(domain={} id={}) Saving resource from {}.".format(
        domain, uid, original_url))
    resource_blob_name = "/".join([blob_prefix, domain, uid, "resource.avro"])
    try:
        records = JSON2AvroRecords(socrata_records(original_url, app_token))
        resource_blob = save_avro_records(records.schema, records.get(), 
                bucket_name, resource_blob_name,
                codec="snappy")
    except Exception as e:
        logger.warning("(domain={} id={}) Failed to save resource from {}: {}".\
                format(domain, id, original_url, e))
        return
    logger.info("(domain={} id={}) Finished saving resource from {} to {}".\
            format(domain, uid, original_url, resource_blob_name))

    # Initialize Postgres connection and cursor for registering resource.
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor()

    # Register this resource.
    cur.execute("INSERT INTO findopendata.socrata_resources "
            "(domain, id, metadata_blob, resource_blob, original_url, "
            "dataset_size) VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (domain, id) DO UPDATE "
            "SET updated = current_timestamp, "
            "metadata_blob = EXCLUDED.metadata_blob, "
            "resource_blob = EXCLUDED.resource_blob, "
            "original_url = EXCLUDED.original_url, "
            "dataset_size = EXCLUDED.dataset_size;",
            (domain, uid, metadata_blob.name, resource_blob.name,
                original_url, resource_blob.size))
    conn.commit()

    # Close database connection for checking resource.
    cur.close()
    conn.close()

    logger.info("(domain={} id={}) Successful.".format(domain, uid))


def _extract_socrata_uid2(metadata):
    # Obtain the resource URL and UID (SODA V2.1) from the web page
    # TODO: check with Scorata see if a fix has been put forward
    # to resolve the 2.0 UID into 2.1 UID.
    # https://github.com/socrata/cetera/issues/260
    domain = metadata["metadata"]["domain"]
    weburl = metadata["permalink"]
    resource_url_reg = re.compile(r"(https?:\/\/" + domain + \
            r"\/resource\/[a-z0-9A-Z]{4}-[a-z0-9A-Z]{4}\.json)")
    uid_reg = re.compile(r"https?:\/\/" + domain + \
            r"\/resource\/([a-z0-9A-Z]{4}-[a-z0-9A-Z]{4})\.json")
    resp = requests.get(weburl)
    resp.raise_for_status()
    items = resource_url_reg.findall(resp.text)
    if len(items) != 1:
        raise RuntimeError("%d resource URL is found from %s: "
                "%s, expecting 1" % (len(items), weburl, items))
    resource_url = items[0]
    uid = uid_reg.findall(resource_url)[0]
    return uid, resource_url


@app.task(ignore_result=True)
def add_socrata_resources_from_api(discovery_api_url, bucket_name, blob_prefix,
        force_update):
    """Scrolls through the Socrata Discovery API and starts tasks to retrieves
    resources.

    Args:
        discovery_api_url: the full URL of the Discovery API endpoint.
        bucket_name: the name of the cloud storage bucket to upload all blobs.
        blob_prefix: the prefix for all the blobs uploaded from this
            function, relative to the root of the bucket.
    """
    logger.info("(discovery_api_url=%s)" % (discovery_api_url))
    app_token = _get_valid_socrata_app_token()
    sources = _process_socrata_raw_metadata(discovery_api_url, 50, app_token)
    for source in sources:
        add_socrata_resource.delay(source, app_token=app_token,
                bucket_name=bucket_name, blob_prefix=blob_prefix,
                force_update=force_update)


def _process_socrata_raw_metadata(discovery_api_url, page_size, token):
    scroll_id = ""
    sess = requests.session()
    while True:
        resp = sess.get(discovery_api_url,
                params={"scroll_id" : scroll_id,
                        "limit" : page_size,
                        "provenance" : "official",
                        "only" : "datasets"},
                headers={"X-App-Token" : token})
        resp.raise_for_status()
        results = resp.json()["results"]
        if len(results) == 0:
            break
        scroll_id = results[-1]["resource"]["id"]
        for metadata in results:
            yield metadata


def _get_valid_socrata_app_token():
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor()
    cur.execute("SELECT token FROM findopendata.socrata_app_tokens "
            "WHERE valid = true ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row is None:
        raise RuntimeError("Cannot found a valid Socrata app token!")
    return row[0]


@app.task(ignore_result=True)
def add_socrata_discovery_apis(force_update):
    """Add Socrata Discovery API endpoints to the crawler."""
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor()
    cur.execute("SELECT url "
            "FROM findopendata.socrata_discovery_apis WHERE enabled = true")
    api_urls = [row[0] for row in cur]
    cur.close()
    conn.close()
    for api_url in api_urls:
        add_socrata_resources_from_api.delay(api_url, 
                gcp_configs.get("bucket_name"),
                crawler_configs.get("socrata_blob_prefix"), force_update)
        logger.info("Adding Socrata Discovery API {} to the crawler".format(
            api_url))
