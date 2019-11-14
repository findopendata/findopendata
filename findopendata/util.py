import os
import shutil
import uuid

from django.utils.text import get_valid_filename
from contextlib import contextmanager


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
