import time

import requests
import dateutil.parser
import dateutil.tz


def read_api(api_url, start=0, page_size=50, retries=3,
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


def parse_timestamp(timestamp_str):
    try:
        timestamp = dateutil.parser.parse(timestamp_str)
        # Set tzinfo to UTC when not available
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=dateutil.tz.tzutc())
    except ValueError:
        return None
    return timestamp


def extract_timestamp_from_package(package):
    package_modified = None
    if "modified" in package:
        package_modified = package["modified"]
    elif "metadata_modified" in package:
        package_modified = package["metadata_modified"]
    elif "metadata_created" in package:
        package_modified = package["metadata_created"]
    if package_modified is None:
        return None
    return parse_timestamp(package_modified)


def extract_timestamp_from_resource(resource):
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
    return parse_timestamp(resource_modified)
