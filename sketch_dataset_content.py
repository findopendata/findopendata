#!/usr/bin/env python
import sys
import argparse
import collections

import psycopg2
from psycopg2.extras import RealDictCursor

from crawler.settings import db_configs, index_configs
from crawler.indexing import sketch_package_file


_sql = r"""
WITH updated_times AS (
    SELECT package_file_key as key, max(updated) as updated
    FROM findopendata.column_sketches
    GROUP BY package_file_key
),
package_files AS (
    SELECT f.key, f.blob_name, f.format, f.updated as file_updated,
        u.updated as sketch_updated
    FROM findopendata.package_files as f
    LEFT JOIN updated_times as u
    ON f.key = u.key
)
SELECT key, blob_name, format
FROM package_files
WHERE (sketch_updated IS NULL OR file_updated > sketch_updated)
    AND blob_name IS NOT NULL
"""


_sql_force_update = r"""
SELECT key, blob_name, format
FROM findopendata.package_files
WHERE blob_name IS NOT NULL
"""


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="Creating column sketches of all package files.")
    parser.add_argument("-u", "--force-update", action="store_true")
    args = parser.parse_args(sys.argv[1:])

    print("Creating tasks (force_update = {})...".format(args.force_update))
    package_files = collections.deque([])
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute((_sql_force_update if args.force_update else _sql))
    for row in cur:
        package_files.append(row)
    cur.close()
    conn.close()
    print("Sending {} tasks to workers.".format(len(package_files)))
    for package_file in package_files:
        fmt = package_file["format"].strip().lower()
        sketch_package_file.delay(package_file_key=package_file["key"],
                blob_name=package_file["blob_name"],
                dataset_format=fmt,
                max_records=index_configs["max_records_per_dataset"],
                table_sample_size=index_configs["table_sample_size"],
                minhash_size=index_configs["minhash_size"],
                minhash_seed=index_configs["minhash_seed"],
                hyperloglog_p=index_configs["hyperloglog_p"],
                column_sample_size=index_configs["column_sample_size"],
                enable_word_vector_data=index_configs["enable_word_vector_data"])
    print("Done sending tasks")
