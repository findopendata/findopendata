#!/usr/bin/env python
import sys
import argparse
import collections

import psycopg2

from crawler.settings import db_configs, gcp_configs
from crawler.indexing import sketch_package_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="Creating column sketches of all package files.")
    parser.add_argument("--minhash-seed", type=int, default=43)
    parser.add_argument("--minhash-size", type=int, default=256)
    parser.add_argument("--hyperloglog-p", type=int, default=8)
    parser.add_argument("--sample-size", type=int, default=100)
    parser.add_argument("--enable-word-vector-data", action="store_true")
    parser.add_argument("--force-update", action="store_true")
    args = parser.parse_args(sys.argv[1:])

    bucket_name = gcp_configs.get("bucket_name")

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
                args.minhash_size, args.minhash_seed, args.hyperloglog_p,
                args.sample_size, args.enable_word_vector_data)
