#!/usr/bin/env python
import sys
import argparse
import collections

import psycopg2
from psycopg2.extras import RealDictCursor

from crawler.settings import db_configs, gcp_configs, index_configs
from crawler.indexing import sketch_package_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="Creating column sketches of all package files.")
    parser.add_argument("--force-update", action="store_true")
    args = parser.parse_args(sys.argv[1:])

    package_files = collections.deque([])
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(r"""SELECT key, created::timestamp, modified::timestamp, 
                        blob_name, format
                    FROM 
                    findopendata.package_files
                    WHERE 
                    blob_name IS NOT NULL
                    """)
    for row in cur:
        package_files.append(row)
    cur.close()
    conn.close()
    print("Sending {} package files to workers.".format(len(package_files)))

    for package_file in package_files:
        if args.force_update:
            modified = None
        elif package_file["modified"] is None:
            modified = package_file["created"]
        fmt = package_file["format"].strip().lower()
        sketch_package_file.delay(package_file_key=package_file["key"], 
                last_modified=modified, 
                bucket_name=gcp_configs["bucket_name"],
                blob_name=package_file["blob_name"], 
                dataset_format=fmt,
                max_records=index_configs["max_records_per_dataset"],
                table_sample_size=index_configs["table_sample_size"],
                minhash_size=index_configs["minhash_size"], 
                minhash_seed=index_configs["minhash_seed"], 
                hyperloglog_p=index_configs["hyperloglog_p"],
                column_sample_size=index_configs["column_sample_size"],
                enable_word_vector_data=index_configs["enable_word_vector_data"])
