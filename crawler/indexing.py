import os

import psycopg2
from psycopg2.extras import Json, RealDictCursor, register_uuid
import dateutil.parser
from celery.utils.log import get_task_logger

from .celery import app
from .settings import db_configs
from .storage import gcs_fs
from .parsers.csv import csv2json
from .parsers.avro import avro2json
from .parsers.jsonl import jsonl2json
from .column_sketch import ColumnSketch
from .table_sketch import TableSketch


logger = get_task_logger(__name__)


def _json_records_sketcher(records, record_sample_size=20, max_records=None, 
        **kwargs):
    table_sketch = TableSketch(record_sample_size=record_sample_size, **kwargs)
    if max_records is not None:
        records = (record for record, _ in zip(records, range(max_records)))
    for record in records:
        table_sketch.update(record)
    return table_sketch


def _csv_sketcher(fileobj_binary, record_sample_size=20, max_records=None, 
        **kwargs):
    records = csv2json(fileobj_binary)
    return _json_records_sketcher(records, record_sample_size, max_records, 
        **kwargs)


def _jsonl_sketcher(fileobj_binary, record_sample_size=20, max_records=None, 
        **kwargs):
    records = jsonl2json(fileobj_binary)
    return _json_records_sketcher(records, record_sample_size, max_records, 
        **kwargs)


def _avro_sketcher(fileobj_binary, record_sample_size=20, max_records=None, 
        **kwargs):
    records = avro2json(fileobj_binary)
    return _json_records_sketcher(records, record_sample_size, max_records, 
        **kwargs)


_sketchers = {
        "csv": _csv_sketcher,
        "jsonl": _jsonl_sketcher,
        "avro": _avro_sketcher,
        }


@app.task(ignore_result=True)
def sketch_package_file(package_file_key, 
        bucket_name, 
        blob_name, 
        dataset_format,
        max_records,
        table_sample_size,
        minhash_size, 
        minhash_seed, 
        hyperloglog_p, 
        column_sample_size,
        enable_word_vector_data):
    """Generate column sketches and table sample of the table in the 
    package file.

    Args:
        package_file_key: the primary key of package_files table.
        bucket_name: the Cloud Storage bucket that stores the package file.
        blob_name: the relative path to the blob of the package file.
        dataset_format: one of csv, jsonl, and avro.
        max_records: the maximum number of records to sketch.
        table_sample_size: the number of records include in the table sample.
        minhash_size: the number of permutation (hash functions) to use for
            MinHash sketches.
        minhash_seed: the random seed for generating MinHash sketches'
            permutations.
        hyperloglog_p: the precision parameter used by HyperLogLog.
        column_sample_size: the number of non-random sampled values.
        enable_word_vector_data: whether to create word vectors for the 
            data values -- this can be 10x more expensive.
    """
    # Build paths
    blob_path = os.path.join(bucket_name, blob_name)

    # Get sketcher
    if dataset_format not in _sketchers:
        raise ValueError("{} is not supported".format(dataset_format))
    sketcher = _sketchers[dataset_format]

    # Sketch the file.
    try:
        with gcs_fs.open(blob_path, "rb") as input_file:
            table_sketch = sketcher(input_file, 
                    record_sample_size=table_sample_size,
                    max_records=max_records,
                    minhash_size=minhash_size, 
                    minhash_seed=minhash_seed,
                    hyperloglog_p=hyperloglog_p,
                    sample_size=column_sample_size,
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
        cur = conn.cursor(cursor_factory=RealDictCursor)
        register_uuid(conn_or_curs=cur) 
        # Save column sketches
        column_sketch_ids = []
        for sketch in table_sketch.column_sketches:
            cur.execute(r"""INSERT INTO findopendata.column_sketches
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
                    SET updated = current_timestamp,
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
                    RETURNING id::uuid
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
            column_sketch_ids.append(cur.fetchone()["id"])
        # Save table samples, column names and column sketch IDs.
        cur.execute(r"""UPDATE findopendata.package_files
                        SET column_names = %s, 
                        column_sketch_ids = %s,
                        sample = %s
                        WHERE key = %s
                        """, (
                            table_sketch.column_names, 
                            column_sketch_ids,
                            Json(table_sketch.record_sample), 
                            package_file_key,
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

