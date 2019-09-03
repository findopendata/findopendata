import os

import psycopg2
import farmhash
import simplejson as json
from datasketch import MinHash
from celery.utils.log import get_task_logger
from gcsfs.core import GCSFileSystem

from .celery import app
from .csv import csv2json
from .avro import avro2json
from .jsonl import jsonl2json


logger = get_task_logger(__name__)


class Sketch:
    def __init__(self, column_name, minhash_size, minhash_seed):
        self._minhash = MinHash(num_perm=minhash_size, seed=minhash_seed,
                hashfunc=self._hashfunc)
        self._column_name = column_name

    def _hashfunc(self, str_value):
        return farmhash.hash32(str_value)

    @property
    def minhash(self):
        return list(int(v) for v in self._minhash.digest())

    @property
    def seed(self):
        return self._minhash.seed

    @property
    def column_name(self):
        return self._column_name

    def update(self, value):
        if isinstance(value, str):
            self._minhash.update(value)
        else:
            self._minhash.update(json.dumps(value, sort_keys=True))


def _json_records_sketcher(records, minhash_size, minhash_seed):
    sketches = {}
    for record in records:
        for column_name, value in record.items():
            if column_name not in sketches:
                sketches[column_name] = Sketch(column_name, minhash_size,
                        minhash_seed)
            sketches[column_name].update(value)
    return sketches.values()


def _csv_sketcher(fileobj_binary, minhash_size, minhash_seed):
    records = csv2json(fileobj_binary)
    return _json_records_sketcher(records, minhash_size, minhash_seed)


def _jsonl_sketcher(fileobj_binary, minhash_size, minhash_seed):
    records = jsonl2json(fileobj_binary)
    return _json_records_sketcher(records, minhash_size, minhash_seed)


def _avro_sketcher(fileobj_binary, minhash_size, minhash_seed):
    records = avro2json(fileobj_binary)
    return _json_records_sketcher(records, minhash_size, minhash_seed)


_sketchers = {
        "csv": _csv_sketcher,
        "jsonl": _jsonl_sketcher,
        "avro": _avro_sketcher,
        }


@app.task(ignore_result=True)
def sketch_package_file(package_file_key, bucket_name, blob_name, dataset_format,
        minhash_size, minhash_seed):
    """Generate column sketches of the table in the package file.

    Args:
        package_file_key: the primary key of package_files table.
        bucket_name: the Cloud Storage bucket that stores the package file.
        blob_name: the relative path to the blob of the package file.
        dataset_format: one of csv, jsonl, and avro
        minhash_size: the number of permutation (hash functions) to use for
            MinHash sketches
        minhash_seed: the random seed for generating MinHash sketches'
            permutations
    """
    # Build paths
    blob_path = os.path.join(bucket_name, blob_name)

    # Get sketcher
    if dataset_format not in _sketchers:
        raise ValueError("{} is not supported".format(dataset_format))
    sketcher = _sketchers[dataset_format]

    # Sketch the file.
    fs = GCSFileSystem()
    try:
        with fs.open(blob_path, "rb") as input_file:
            sketches = sketcher(input_file, minhash_size, minhash_seed)
    except Exception as e:
        logger.error("Sketching {} ({}) failed due to {}".format(
            blob_path, package_file_key, e))
        raise e

    try:
        # Save sketches to the database
        # Initialize Postgres connection.
        conn = psycopg2.connect("")
        cur = conn.cursor()
        # Save
        for sketch in sketches:
            cur.execute("""INSERT INTO findopendata.column_sketches
                    (package_file_key, column_name, id, minhash, seed)
                    VALUES (%s, %s, uuid_generate_v1mc(), %s, %s)
                    ON CONFLICT (package_file_key, column_name)
                    DO UPDATE
                    SET modified = current_timestamp,
                    minhash = EXCLUDED.minhash,
                    seed = EXCLUDED.seed;""", (package_file_key,
                        sketch.column_name, sketch.minhash,
                        sketch.seed))
        # Commit
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error("Error saving sketches of {} ({}) due to {}".format(
            blob_path, package_file_key, e))
        raise e

    # Finish
    logger.info("Sketching {} ({}) successful".format(blob_path,
        package_file_key))

