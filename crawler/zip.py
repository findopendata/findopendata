import os
import zipfile

from .util import get_mime


def is_zipfile(local_filename):
    mimetype, _ = get_mime(local_filename)
    return mimetype == "application/zip"


def unzip(local_filename, working_dir):
    """Unzips the local file into a list of files.

    Args:
        local_filename: the path to the local zip file to unzip.
        working_dir: the parent directory for unzipping files.

    Returns: a list of paths relative to the working_dir of the unzipped files.
    """
    z = zipfile.ZipFile(local_filename)
    prefix, _ = os.path.splitext(os.path.basename(local_filename))
    unzipped_files = []
    # Create the parent directory for unzipped files.
    os.mkdir(os.path.join(working_dir, prefix))
    for name in z.namelist():
        # TODO: In Python 3.6 we can use is_dir() on ZipInfo objects.
        if name.endswith("/"):
            continue
        z.extract(name, path=os.path.join(working_dir, prefix))
        unzipped_files.append(os.path.join(prefix, name))
    return unzipped_files
