import csv
import io
import itertools
from collections import OrderedDict

from .encoding import guess_encoding_from_buffer


_sniffer = csv.Sniffer()


def _is_number(x):
    try:
        a = float(x)
    except ValueError:
        return False
    else:
        return True


def csv2json(fileobj_binary, guess_encoding_bytes=8192, guess_header_rows=10,
        header_prefix="Column-"):
    """Read a CSV file and get an iterator of JSON records as Python dictionaries.

    Args:
        fileobj_binary: a binary file object that supports seek().
        guess_encoding_bytes: the number of bytes in the beginning of the file
            to be used to guess the text encoding.
        header_prefix: the string prefix used for assigning headers
            to CSV tables without a header row.

    Returns: an iterator of JSON records as Python dictionaries.
    """

    # Guess encoding
    head = fileobj_binary.read(guess_encoding_bytes)
    encoding = guess_encoding_from_buffer(head)

    # Guess dialect and headers
    dialect = _sniffer.sniff(head.decode(encoding))

    # Rewind
    fileobj_binary.seek(0)

    # Wrap the binary file with text file reader to create csv reader
    fileobj = io.TextIOWrapper(fileobj_binary, encoding=encoding, newline='')
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

    # Yield records.
    rows = itertools.chain(head[header_row_pos+1:], reader)
    for row in rows:
        yield OrderedDict(zip(headers, row))
