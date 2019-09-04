import os
import shutil
import re
import uuid
import cgi

import magic
import cchardet as chardet
from django.utils.text import get_valid_filename
from contextlib import contextmanager


content_type_re = re.compile(r"^[^/]+/[^/]+$")


magic_encoding = magic.Magic(mime_encoding=True)


magic_parser = magic.Magic(mime=True, mime_encoding=True)


magic_parser_uncompress = magic.Magic(mime=True, mime_encoding=True,
        uncompress=True)


def get_safe_filename(s, default_filename="unnamed_file"):
    """Returns a sanitized filename from the one given.
    """
    if not s:
        return default_filename
    if len(s) > 255:
        s, ext = os.path.splitext(s)
        ext = ext[:255]
        s = s[:255 - len(ext)]+ ext
    s = s.replace(" ", "_")
    s = get_valid_filename(s).strip(".")
    if len(s) == 0:
        return default_filename
    return s


@contextmanager
def temporary_directory(parent_dir):
    """Creates a temporary directory using with. For example:

        with temporary_directory("/tmp/parent_dir") as dir_name:
            # Do things in the newly created dir_name.
            # ...

        # Now the directory dir_name is removed.
    """
    dir_name = os.path.join(parent_dir, str(uuid.uuid4()))
    os.makedirs(dir_name, exist_ok=True)
    try:
        yield dir_name
    finally:
        shutil.rmtree(dir_name)


def get_mime(filename, guess_encoding_bytes=8192, uncompress=False):
    """Get the Mimetype from a file, and optionally handles compressed
    file by setting uncompress=True.
    """
    parser = magic_parser
    if uncompress:
        parser = magic_parser_uncompress
    mimetype, params = cgi.parse_header(parser.from_file(filename))
    encoding = params["charset"]
    if encoding.startswith("unknown") and not uncompress:
        with open(filename, "rb") as f:
            result = chardet.detect(f.read(guess_encoding_bytes))
        encoding = result["encoding"]
    return mimetype, encoding


def get_mime_buffer(buf, uncompress=False):
    """Get the Mimetype from buffer, and optionally handles compressed
    file by setting uncompress=True.
    """
    parser = magic_parser
    if uncompress:
        parser = magic_parser_uncompress
    mimetype, params = cgi.parse_header(parser.from_buffer(buf))
    encoding = params["charset"]
    if encoding.startswith("unknown") and not uncompress:
        result = chardet.detect(buf)
        encoding = result["encoding"]
    return mimetype, encoding


def guess_encoding_from_buffer(buf, chardet_threshold=0.5):
    result = chardet.detect(buf)
    if result["confidence"] < chardet_threshold:
        # Try magic
        encoding = magic_encoding.from_buffer(buf)
        if not encoding.startswith("unknown"):
            return encoding
    return result["encoding"]

