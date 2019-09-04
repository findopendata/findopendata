import io
import os
import warnings

from google.oauth2 import service_account
from google.cloud import storage
from gcsfs.core import GCSFileSystem
import simplejson as json
import fastavro

from .settings import gcp_configs
from .util import get_mime_buffer
from .gzip import Bytes2GzipStreamReader


# The chunk size for uploading
chunk_size = 1024*1024*5

# GCP Service account credentials.
gcp_credentials = service_account.Credentials.from_service_account_file(
        gcp_configs.get("service_account_file"))

# The Google Cloud Storage client.
gcs_client = storage.Client(
        project=gcp_configs.get("project_id"),
        credentials=gcp_credentials)

# The GCSFileSystem interface
gcs_fs = GCSFileSystem(
        token=gcp_configs.get("service_account_file"), 
        check_connection=True)


def get_object(bucket_name, blob_name):
    """Get a JSON object from bucket as a Python dictionary.
    
    Args:
        bucket_name: the name of the bucket.
        blob_name: the name of the blob object relative to the bucket.
    
    Returns: a Python dictionary.
    """
    blob = gcs_client.bucket(bucket_name).get_blob(blob_name)
    if blob is None:
        raise ValueError("Cannot find blob {}/{}".format(bucket_name, blob_name))
    return json.loads(blob.download_as_string().decode("utf-8"))


def save_file(fileobj, bucket_name, blob_name, 
        guess_content_bytes=8192,
        gzip=False, 
        public=False):
    """Uploads a file to the bucket.

    Args:
        fileobj: the file object to be uploaded to the bucket.
        bucket_name: the name of the destination bucket.
        blob_name: the name of the destination blob object relative to the
            bucket.
        guess_content_btypes: number of bytes for guess content type and 
            encoding.
        gzip (default True): whether to gzip compress the input fileobj.
        public (default False): whether to make the resulting blob object
            public.

    Returns:
        blob: the storage.Blob object that was created.
    """
    blob = gcs_client.bucket(bucket_name).blob(blob_name, chunk_size=chunk_size)
    # Check the content type (i.e. mime type) and content encoding
    head = fileobj.read(guess_content_bytes)
    mimetype, encoding = get_mime_buffer(head)
    blob.content_type = mimetype
    blob.content_encoding = encoding
    fileobj.seek(0)
    # Wrapp with gzip if set
    if gzip:
        blob.content_encoding = "gzip"
        fileobj = Bytes2GzipStreamReader(fileobj, chunk_size=chunk_size*2)
    # Upload
    blob.upload_from_file(fileobj)
    if public:
        blob.make_public()
    blob.reload()
    return blob


def save_avro_records(schema, records, bucket_name, blob_name, 
        codec="snappy",
        public=False):
    """Uplaods Avro records.

    Args:
        blob_name: the name of the destination blob object relative to the
            bucket.
        schema: the Avro schema to use.
        records: an iterator of records of type `dict`.
        codec: the compression codec to use (e.g., `snappy`).

    Returns:
        blob: the storage.Blob object that was created.
    """
    blob_path = os.path.join(bucket_name, blob_name)
    tmp_blob_path = os.path.join(os.path.dirname(blob_path),
            "~{}".format(os.path.basename(blob_path)))
    with gcs_fs.open(tmp_blob_path, "wb") as of:
        fastavro.writer(of, schema, records, codec)
    gcs_fs.mv(tmp_blob_path, blob_path)
    gcs_fs.setxattrs(blob_path, content_type="avro/binary")
    blob = gcs_client.bucket(bucket_name).get_blob(blob_name)
    if blob is None:
        raise RuntimeError("Cannot find new avro blob {}/{}".format(
            bucket_name, blob_name))
    if public:
        blob.make_public()
    return blob


def save_object(obj, bucket_name, blob_name, 
        gzip=False, 
        public=False):
    """Uploads a single JSON-serializable Python object to the bucket.

    Args:
        obj: the object to be JSON-serialized and saved to the bucket.
            blob
        bucket_name: the name of the destination bucket.
        blob_name: the name of the destination blob object relative to the
            bucket.
        gzip (default True): whether to gzip compress the input stream.
        public (default False): whether to make the resulting blob object
            public.

    Returns:
        blob: the storage.Blob object that was created.
    """
    blob = gcs_client.bucket(bucket_name).blob(blob_name, chunk_size=chunk_size)
    blob.content_type = "application/json"
    data = io.BytesIO(json.dumps(obj).encode("utf-8"))
    if gzip:
        blob.content_encoding = "gzip"
        f = Bytes2GzipStreamReader(data, chunk_size=chunk_size*2)
        blob.upload_from_file(f)
    else:
        blob.upload_from_file(data)
    if public:
        blob.make_public()
    blob.reload()
    return blob
