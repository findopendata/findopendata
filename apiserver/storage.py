from gcsfs.core import GCSFileSystem

from crawler.csv import csv2json


_fs = GCSFileSystem()


def _read_csv(bucket_name, blob_name, max_rows=20, guess_encoding_bytes=8192,
        guess_header_rows=10, header_prefix="Column_"):
    with _fs.open("{}/{}".format(bucket_name, blob_name), "rb") as csv_file:
        records = csv2json(csv_file, 
                guess_encoding_bytes=guess_encoding_bytes,
                guess_header_rows=guess_header_rows, 
                header_prefix=header_prefix)
        return list(record for record, _ in zip(records, range(max_rows)))


def read_package_file(fmt, bucket_name, blob_name, max_rows):
    fmt = fmt.strip().lower()
    if fmt == "csv":
        records = _read_csv(bucket_name, blob_name, max_rows)
        headers = list(records[0].keys())
        return headers, [dict(record) for record in records]
    raise RuntimeError("Format {} is not supported.".format(fmt))

