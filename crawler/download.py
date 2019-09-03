import os
import shutil

import rfc6266
import requests

from .util import get_safe_filename

def download_to_local(url, dir_name):
    """Downloads remote resource given its URL.

    Args:
        url: the URL to the resource.
        dir_name: the directory on the local filesystem to save the resource.

    Returns:
        filename: the filename (relative to the dir_name) of the downloaded
            resource. It may be different from the name of the remote resource
            because of sanitization.
    """

    # TODO: be able to verify SSL certificates from some publishers
    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        # Guesst the proper filename to use
        filename = ""
        # 1. Try to use the content-disposition header if available
        if "content-disposition" in r.headers:
            filename = rfc6266.parse_headers(r.headers["content-disposition"],
                    relaxed=True).filename_unsafe
        # 2. Try to get it from the URL
        if filename == "":
            filename = url.rsplit("/", 1)[-1]
        # 3. Sanitize the filename, this handles empty filename right now
        filename = get_safe_filename(filename)

        # Download the file
        with open(os.path.join(dir_name, filename), "wb") as o:
            shutil.copyfileobj(r.raw, o)
    return filename
