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
    parser.add_argument("--sample-size", type=int, default=100)
    args = parser.parse_args(sys.argv[1:])

    bucket_name = gcp_configs.get("bucket_name")

    package_files = collections.deque([])
    conn = psycopg2.connect(**db_configs)
    cur = conn.cursor()
    cur.execute(r"""SELECT key, blob_name, format 
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

    for key, blob_name, fmt in package_files:
        fmt = fmt.strip().lower()
        sketch_package_file.delay(key, bucket_name, blob_name, fmt,
                args.minhash_size, args.minhash_seed, args.sample_size)
