import io

import simplejson as json

from .encoding import guess_encoding_from_buffer


def jsonl2json(fileobj_binary, guess_encoding_bytes=8192):
    """Read a JSONL (newline-delimited JSON) file and get an iterator of
    JSON records as Python dictionaries.

    Args:
        fileobj_binary: a binary file object that supports seek().
        guess_encoding_bytes: the number of bytes in the beginning of the file
            to be used to guess the text encoding.

    Returns: an iterator of JSON records as Python dictionaries.
    """

    # Guess encoding
    head = fileobj_binary.read(guess_encoding_bytes)
    encoding = guess_encoding_from_buffer(head)

    # Rewind
    fileobj_binary.seek(0)

    # Wrap the binary file with text file reader to create line reader
    fileobj = io.TextIOWrapper(fileobj_binary, encoding=encoding, newline='')

    # Return records
    for line in fileobj:
        yield json.loads(line)
