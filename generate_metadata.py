#!/usr/bin/env python
import sys
import argparse
import collections

import psycopg2

from crawler.settings import db_configs, gcp_configs
from crawler.metadata import index_ckan_package, index_socrata_resource


_sql_ckan_force_update = r"""
SELECT p.key, p.package_blob, p.endpoint
FROM findopendata.original_hosts as h, findopendata.ckan_packages as p
WHERE p.endpoint = h.original_host and h.enabled
"""

_sql_ckan = r"""
WITH updated_times AS (
    SELECT crawler_key as key, updated
    FROM findopendata.packages
    WHERE crawler_table = 'ckan_packages'
),
enabled_packages AS (
    SELECT p.key, p.package_blob, p.endpoint
    FROM findopendata.original_hosts as h, findopendata.ckan_packages as p
    WHERE p.endpoint = h.original_host and h.enabled
),
packages AS (
    SELECT a.key, a.package_blob, a.endpoint, a.updated as crawler_updated, 
        u.updated as package_updated
    FROM enabled_packages AS a
    LEFT JOIN updated_times AS u
    ON a.key = u.key
)
SELECT key, package_blob, endpoint
FROM packages
WHERE package_updated IS NULL OR crawler_updated > package_updated
"""

_sql_socrata_force_update = r"""
SELECT r.key, r.domain, r.metadata_blob, r.resource_blob, r.dataset_size
FROM findopendata.socrata_resources as r, findopendata.original_hosts as h
WHERE r.domain = h.original_host AND h.enabled
"""

_sql_socrata = r"""
WITH updated_times AS (
    SELECT crawler_key as key, updated
    FROM findopendata.packages
    WHERE crawler_table = 'socrata_resources'
),
enabled_resources AS (
    SELECT r.key, r.domain, r.metadata_blob, r.resource_blob, r.dataset_size
    FROM findopendata.socrata_resources as r, findopendata.original_hosts as h
    WHERE r.domain = h.original_host AND h.enabled
),
resources AS (
    SELECT a.key, a.domain, a.metadata_blob, a.resource_blob,
        a.dataset_size, a.updated as crawler_updated,
        u.updated as package_updated
    FROM enabled_resources as a
    LEFT JOIN updated_times as u
    ON a.key = u.key
)
SELECT key, domain, metadata_blob, resource_blob, dataset_size
FROM resources
WHERE package_updated IS NULL OR crawler_updated > package_updated
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
    cur.execute((_sql_ckan_force_update if args.force_update else _sql_ckan))
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
            bucket_name=gcp_configs["bucket_name"])
    print("Done sending CKAN tasks.")
    ckan_packages.clear()

    # Socrata
    print("Creating Socrata tasks (force_update = {})...".format(args.force_update))
    socrata_resources = collections.deque([])
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor()
    cur.execute((_sql_socrata_force_update if args.force_update else _sql_socrata))
    for row in cur:
        socrata_resources.append(row)
    cur.close()
    conn.close()
    print("Sending {} Socrata tasks to workers...".format(len(socrata_resources)))
    for key, domain, metadata_blob, resource_blob, dataset_size in \
            socrata_resources:
        index_socrata_resource.delay(
            crawler_key=key,
            domain=domain,
            metadata_blob_name=metadata_blob,
            resource_blob_name=resource_blob,
            dataset_size=dataset_size,
            bucket_name=gcp_configs["bucket_name"])
    print("Done sending Socrata tasks.")
    socrata_resources.clear()
