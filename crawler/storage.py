import io
import os
import warnings

from google.cloud import storage
from gcsfs.core import GCSFileSystem
import simplejson as json
import fastavro

from .util import get_mime_buffer
from .gzip import Bytes2GzipStreamReader


# The chunk size for uploading
chunk_size = 1024*1024*5


class CloudStorageError(Exception):
    pass


class CloudStorageBucket:
    """A wrapper class that implements methods for reading and saving data
    to the storage bucket."""

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def list_blobs(self, prefix):
        return self.bucket.list_blobs(prefix=prefix)

    def get_object(self, blob_name):
        blob = self.bucket.get_blob(blob_name)
        if blob is None:
            raise CloudStorageError("Bucket %s: cannot find blob %s" %
                    (self.bucket.name, blob_name))
        return json.loads(blob.download_as_string().decode("utf-8"))

    def save_file(self, fileobj, blob_name, guess_content_bytes=8192,
            gzip=False, public=False):
        """Uploads a file to the bucket.
        """
        blob = self.bucket.blob(blob_name, chunk_size=chunk_size)
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

    def save_avro_records(self, blob_name, schema, records, codec="snappy",
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
        fs = GCSFileSystem()
        blob_path = os.path.join(self.bucket_name, blob_name)
        tmp_blob_path = os.path.join(os.path.dirname(blob_path),
                "~{}".format(os.path.basename(blob_path)))
        with fs.open(tmp_blob_path, "wb") as of:
            fastavro.writer(of, schema, records, codec)
        fs.mv(tmp_blob_path, blob_path)
        fs.setxattrs(blob_path, content_type="avro/binary")
        blob = self.bucket.get_blob(blob_name)
        if blob is None:
            raise CloudStorageError("Bucket %s: cannot find new avro blob %s" %
                    (self.bucket.name, blob_name))
        if public:
            blob.make_public()
        return blob

    def save_object(self, obj, blob_name, gzip=False, public=False):
        """Uploads a single JSON-serializable Python object to the bucket.

        Args:
            obj: the object to be JSON-serialized and saved to the bucket.
            blob
            blob_name: the name of the destination blob object relative to the
                bucket.
            gzip (default True): whether to gzip compress the input stream.
            public (default False): whether to make the resulting blob object
            public.

        Returns:
            blob: the storage.Blob object that was created.
        """
        blob = self.bucket.blob(blob_name, chunk_size=chunk_size)
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

