#!/usr/bin/env python
import sys
import argparse
import collections

import psycopg2

from crawler.settings import db_configs, gcp_configs, index_configs
from crawler.indexing import sketch_package_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="Creating column sketches of all package files.")
    parser.add_argument("--force-update", action="store_true")
    args = parser.parse_args(sys.argv[1:])

    bucket_name = gcp_configs["bucket_name"]
    max_records_per_dataset = index_configs["max_records_per_dataset"]
    minhash_seed = index_configs["minhash_seed"]
    minhash_size = index_configs["minhash_size"]
    hyperloglog_p = index_configs["hyperloglog_p"]
    sample_size = index_configs["sample_size"]
    enable_word_vector_data = index_configs["enable_word_vector_data"]

    package_files = collections.deque([])
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor()
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

    for key, created, modified, blob_name, fmt in package_files:
        if args.force_update:
            modified = None
        elif modified is None:
            modified = created
        fmt = fmt.strip().lower()
        sketch_package_file.delay(key, modified, bucket_name, blob_name, fmt,
                max_records_per_dataset,
                minhash_size, minhash_seed, hyperloglog_p,
                sample_size, enable_word_vector_data)
