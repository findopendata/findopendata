import csv
import io
import itertools

from gcsfs.core import GCSFileSystem
import magic
import chardet


_magic_parser = magic.Magic(mime_encoding=True)
_sniffer = csv.Sniffer()
_fs = GCSFileSystem()


def _get_encoding_from_buffer(buf):
    encoding = _magic_parser.from_buffer(buf)
    if encoding.startswith("unknown"):
        # Fallback to use chardet (slower)
        result = chardet.detect(buf)
        encoding = result["encoding"]
    return encoding


def _is_number(x):
    try:
        _ = float(x)
    except ValueError:
        return False
    else:
        return True


def _read_csv(bucket_name, blob_name, max_rows=20, guess_encoding_bytes=8192,
        guess_header_rows=10, header_prefix="Column_"):
    with _fs.open("{}/{}".format(bucket_name, blob_name), "rb") as csv_file:
        # Guess encoding
        head = csv_file.read(guess_encoding_bytes)
        encoding = _magic_parser.from_buffer(head)
        if encoding.startswith("unknown"):
            # Fallback to use chardet (slower)
            result = chardet.detect(head)
            encoding = result["encoding"]

        # Guess dialect and headers
        dialect = _sniffer.sniff(head.decode(encoding))

        # Rewind
        csv_file.seek(0)

        # Wrap the binary file with text file reader to create csv reader
        fileobj = io.TextIOWrapper(csv_file, encoding=encoding, newline='')
        reader = csv.reader(fileobj, dialect)

        # Figure out the first row that looks like a header using the
        # first few rows.
        head = [row for _, row in zip(range(guess_header_rows), reader)]
        headers = None
        header_row_pos = 0
        for i, row in enumerate(head):
            if all(len(v.strip()) > 0 and not _is_number(v) for v in row):
                headers = row
                header_row_pos = i
                break
        # Assign default headers if none found
        if headers is None:
            ncol = max(len(row) for row in head)
            headers = ["{}{}".format(header_prefix, i) for i in range(ncol)]
            header_row_pos = -1
        yield headers

        # Yield records.
        row_count = 0
        rows = itertools.chain(head[header_row_pos+1:], reader)
        for row in rows:
            yield row
            row_count += 1
            if row_count >= max_rows:
                return


def read_package_file(fmt, bucket_name, blob_name, max_rows):
    fmt = fmt.strip().lower()
    if fmt == "csv":
        rows = _read_csv(bucket_name, blob_name, max_rows)
        headers = next(rows)
        return headers, list(dict(zip(headers, row)) for row in rows)
    raise RuntimeError("Format {} is not supported.".format(fmt))

