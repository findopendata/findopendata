import os

import psycopg2
import dateutil.parser
from celery.utils.log import get_task_logger

from .celery import app
from .settings import db_configs
from .storage import gcs_fs
from .parsers.csv import csv2json
from .parsers.avro import avro2json
from .parsers.jsonl import jsonl2json
from .column_sketch import ColumnSketch


logger = get_task_logger(__name__)


def _json_records_sketcher(records, **kwargs):
    sketches = {}
    for record in records:
        for column_name, value in record.items():
            if column_name not in sketches:
                sketches[column_name] = ColumnSketch(column_name, **kwargs)
            sketches[column_name].update(value)
    return sketches.values()


def _csv_sketcher(fileobj_binary, **kwargs):
    records = csv2json(fileobj_binary)
    return _json_records_sketcher(records, **kwargs)


def _jsonl_sketcher(fileobj_binary, **kwargs):
    records = jsonl2json(fileobj_binary)
    return _json_records_sketcher(records, **kwargs)


def _avro_sketcher(fileobj_binary, **kwargs):
    records = avro2json(fileobj_binary)
    return _json_records_sketcher(records, **kwargs)


_sketchers = {
        "csv": _csv_sketcher,
        "jsonl": _jsonl_sketcher,
        "avro": _avro_sketcher,
        }


@app.task(ignore_result=True)
def sketch_package_file(package_file_key, last_modified,
        bucket_name, blob_name, dataset_format,
        minhash_size, minhash_seed, hyperloglog_p, sample_size,
        enable_word_vector_data):
    """Generate column sketches of the table in the package file.

    Args:
        package_file_key: the primary key of package_files table.
        last_modified: the datetime of the last modification of this 
                package file.
        bucket_name: the Cloud Storage bucket that stores the package file.
        blob_name: the relative path to the blob of the package file.
        dataset_format: one of csv, jsonl, and avro.
        minhash_size: the number of permutation (hash functions) to use for
            MinHash sketches.
        minhash_seed: the random seed for generating MinHash sketches'
            permutations.
        hyperloglog_p: the precision parameter used by HyperLogLog.
        sample_size: the number of non-random sampled values.
        enable_word_vector_data: whether to create word vectors for the 
            data values -- this can be 10x more expensive.
    """
    # Check the latest update time of this package file.
    if last_modified is not None:
        # Deserialize the timestamp, as Celery JSON serialize does not support
        # datetime type.
        if isinstance(last_modified, str):
            last_modified = dateutil.parser.parse(last_modified)
        conn = psycopg2.connect(**db_configs)
        cur = conn.cursor()
        cur.execute(r"""SELECT max(modified)
                    FROM findopendata.column_sketches
                    WHERE package_file_key = %s
                    """, (package_file_key,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row is not None:
            last_sketched = row[0]
            if last_sketched is not None and last_modified <= last_sketched:
                logger.info("Skip sketching package file {}".format(
                    package_file_key))
                return

    # Build paths
    blob_path = os.path.join(bucket_name, blob_name)

    # Get sketcher
    if dataset_format not in _sketchers:
        raise ValueError("{} is not supported".format(dataset_format))
    sketcher = _sketchers[dataset_format]

    # Sketch the file.
    try:
        with gcs_fs.open(blob_path, "rb") as input_file:
            sketches = sketcher(input_file, 
                    minhash_size=minhash_size, 
                    minhash_seed=minhash_seed,
                    hyperloglog_p=hyperloglog_p,
                    sample_size=sample_size,
                    enable_word_vector_data=enable_word_vector_data
                    )
    except Exception as e:
        logger.error("Sketching {} ({}) failed due to {}".format(
            blob_path, package_file_key, e))
        raise e

    try:
        # Save sketches to the database
        # Initialize Postgres connection.
        conn = psycopg2.connect(**db_configs)
        cur = conn.cursor()
        # Save
        for sketch in sketches:
            cur.execute("""INSERT INTO findopendata.column_sketches
                    (
                        package_file_key, 
                        id, 
                        column_name,
                        sample,
                        count,
                        empty_count,
                        out_of_vocabulary_count,
                        numeric_count,
                        distinct_count,
                        word_vector_column_name,
                        word_vector_data,
                        minhash, 
                        seed,
                        hyperloglog
                    )
                    VALUES (%s, uuid_generate_v1mc(), 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (package_file_key, column_name)
                    DO UPDATE
                    SET modified = current_timestamp,
                    sample = EXCLUDED.sample,
                    count = EXCLUDED.count,
                    empty_count = EXCLUDED.empty_count,
                    out_of_vocabulary_count = EXCLUDED.out_of_vocabulary_count,
                    numeric_count = EXCLUDED.numeric_count,
                    distinct_count = EXCLUDED.distinct_count,
                    word_vector_column_name = EXCLUDED.word_vector_column_name,
                    word_vector_data = EXCLUDED.word_vector_data,
                    minhash = EXCLUDED.minhash,
                    seed = EXCLUDED.seed,
                    hyperloglog = EXCLUDED.hyperloglog
                    """, (
                        package_file_key,
                        sketch.column_name, 
                        sketch.sample,
                        sketch.count,
                        sketch.empty_count,
                        sketch.out_of_vocabulary_count,
                        sketch.numeric_count,
                        sketch.distinct_count,
                        sketch.word_vector_column_name,
                        sketch.word_vector_data,
                        sketch.minhash,
                        sketch.seed,
                        sketch.hyperloglog,
                        ))
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
