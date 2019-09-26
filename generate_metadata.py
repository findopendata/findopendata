#!/usr/bin/env python
import sys
import argparse
import collections

import psycopg2

from crawler.settings import db_configs, gcp_configs
from crawler.metadata import index_ckan_package

_endpoints = (
    "open.canada.ca/data/en",
    "catalog.data.gov",
    "data.gov.uk",
)

_sql_ckan_force_update = r"""
SELECT b.key, b.package_blob, a.endpoint, a.name, a.region
FROM findopendata.ckan_apis as a, findopendata.ckan_packages as b
WHERE a.endpoint = b.endpoint AND a.endpoint in %s
"""

_sql_ckan = r"""
WITH updated_times AS (
    SELECT crawler_key as key, updated
    FROM findopendata.packages
)
SELECT b.key, b.package_blob, a.endpoint, a.name, a.region
FROM findopendata.ckan_apis as a, findopendata.ckan_packages as b,
    updated_times as u
WHERE a.endpoint = b.endpoint AND a.endpoint in %s 
AND b.key = u.key AND b.updated > u.updated
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract and generate package metadata and "
        "make the packages searchable.")
    parser.add_argument("-u", "--force-update", action="store_true")
    args = parser.parse_args(sys.argv[1:])

    # CKAN
    print("Creating CKAN tasks (force_update = {})...".format(args.force_update))
    ckan_packages = collections.deque([])
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor()
    cur.execute((_sql_ckan_force_update if args.force_update else _sql_ckan),
            (_endpoints,))
    for row in cur:
        ckan_packages.append(row)
    cur.close()
    conn.close()
    print("Sending {} CKAN tasks to workers...".format(len(ckan_packages)))
    for key, package_blob, endpoint, endpoint_name, endpoint_region in \
            ckan_packages:
        index_ckan_package.delay(
            crawler_package_key=key, 
            package_blob_name=package_blob, 
            endpoint=endpoint,
            endpoint_name=endpoint_name,
            endpoint_region=endpoint_region,
            bucket_name=gcp_configs["bucket_name"])
    print("Done sending CKAN tasks.")
