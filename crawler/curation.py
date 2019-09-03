import os
import shutil
import gzip

from celery.utils.log import get_task_logger
from gcsfs.core import GCSFileSystem
from google.cloud import storage

from .celery import app
from .util import temporary_directory


# The directory for working on resource download.
working_dir = os.getenv("FINDOPENDATA_CRAWLER_WDIR", "tmp")

logger = get_task_logger(__name__)


@app.task(ignore_result=True)
def gzip_compress_blob(bucket_name, blob_name, compressed_blob_name=None):
    """Compress blob using Gzip compression.

    Args:
        bucket_name: the name of the cloud storage bucket that stores the blobs.
        blob_name: the path (without bucket name) to the blob to be compressed.
        compressed_blob_name: the path (without bucket name) to the output
            compressed blob. Replace the original blob if not specified.
    """
    # Build paths
    blob_path = os.path.join(bucket_name, blob_name)
    if compressed_blob_name is None:
        compressed_blob_path = blob_path
    else:
        compressed_blob_path = os.path.join(bucket_name, compressed_blob_name)
    tmp_blob_path = os.path.join(os.path.dirname(blob_path),
            "~{}".format(os.path.basename(blob_path)))
    # Check if the file is already gzip-compressed
    fs = GCSFileSystem()
    encoding = fs.info(blob_path).get("contentEncoding", None)
    if encoding is not None and encoding == "gzip":
        logger.error("{} is already gzip-compressed".format(blob_path))
        return False
    # Compress blob
    try:
        with fs.open(blob_path, "rb") as input_file, \
                fs.open(tmp_blob_path, "wb") as output_file:
            with gzip.open(output_file, "wb") as of:
                shutil.copyfileobj(input_file, of)
        fs.mv(tmp_blob_path, compressed_blob_path)
    except Exception as e:
        logger.error("Compressing {} failed due to {}".format(blob_path, e))
        return False
    # Update content encoding of the blob
    fs.setxattrs(compressed_blob_path, content_encoding="gzip")
    logger.info("Compressing {} successful")
    return True


@app.task(ignore_result=True)
def gzip_decompress_blob(bucket_name, blob_name, output_blob_name=None):
    """Decompress Gzip-compressed blob.

    Args:
        bucket_name: the name of the cloud storage bucket that stores the blobs.
        blob_name: the path (without bucket name) to the blob to be decompressed.
        output_blob_name: the path (without bucket name) to the output
            decompressed blob. Replace the original blob if not specified.
    """
    if output_blob_name is None:
        output_blob_name = blob_name
    with temporary_directory(working_dir) as parent_dir:
        # Get connection
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name)
        if blob is None:
            logger.error("{} does not exist".format(blob_name))
            return False
        # Check if the blob is not compressed
        if blob.content_encoding != "gzip":
            logger.error("{} is not gzip-compressed".format(blob_name))
            return False
        # Download the file, while compressing it to local disk
        # NOTE: because range header is not supported for gzip-compressed
        # blobs, we have to download the whole file before uploading it.
        filename = os.path.join(parent_dir, os.path.basename(blob_name))
        try:
            with gzip.open(filename, "wb") as f:
                blob.download_to_file(f)
        except Exception as e:
            logger.error("Downloading {} failed due to {}".format(blob_name, e))
            return False
        # Uploading to output blob
        if output_blob_name != blob_name:
            blob = bucket.blob(output_blob_name)
        # Set content encoding
        blob.content_encoding = None
        # Upload while decompressing
        try:
            with gzip.open(filename, "rb") as f:
                blob.upload_from_file(f)
        except Exception as e:
            logger.error("Uploading to {} failed due to {}".format(
                output_blob_name, e))
            return False
        logger.info("Decompressing {} successful".format(blob_name))
        return True

