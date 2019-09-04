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
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="Extract and index package metadata from crawler "
            "dumps to make the packages searchable.")
    args = parser.parse_args(sys.argv[1:])

    bucket_name = gcp_configs.get("bucket_name")

    ckan_packages = collections.deque([])
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor()
    cur.execute("""SELECT 
                b.key, b.package_blob,
                a.endpoint, a.name, a.region
                FROM 
                findopendata.ckan_apis as a, 
                findopendata.ckan_packages as b
                WHERE 
                a.endpoint = b.endpoint
                AND a.endpoint in %s
                """, (_endpoints,))
    for row in cur:
        ckan_packages.append(row)
    cur.close()
    conn.close()
    print("Sending {} CKAN packages to workers.".format(len(ckan_packages)))

    # CKAN
    for key, package_blob, endpoint, endpoint_name, endpoint_region in \
                ckan_packages:
        index_ckan_package.delay(
                crawler_package_key=key, 
                package_blob_name=package_blob, 
                endpoint=endpoint,
                endpoint_name=endpoint_name,
                endpoint_region=endpoint_region,
                bucket_name=bucket_name)
