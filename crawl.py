#!/usr/bin/env python
import argparse
import sys

from crawler.ckan_crawler import add_ckan_apis
from crawler.socrata_crawler import add_socrata_discovery_apis

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="Start crawling tasks.")
    parser.add_argument("-u", "--force-update", action='store_true',
            help="Whether to force update a dataset even if the modified time "
                "is prior to the last updated time.")
    args = parser.parse_args(sys.argv[1:])

    # CKAN
    add_ckan_apis.delay(force_update=args.force_update)
    # Socrata
    add_socrata_discovery_apis.delay(force_update=args.force_update)
