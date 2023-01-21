import io
import base64
import contextlib
import urllib
import collections
import gzip

import fastavro
import simplejson as json
from azure.storage.blob import BlobBlock, ContainerClient, BlobClient

from .base import Blob, BlobStorage


def _encode_base64(data):
    if isinstance(data, str):
        data = data.encode("utf-8")
    encoded = base64.b64encode(data)
    return encoded.decode("utf-8")


def _block_id(offset):
    return urllib.parse.quote(_encode_base64("{0:032d}".format(offset)))


class AzureBlobReader(io.BufferedIOBase):
    def __init__(
        self,
        blob_client: BlobClient,
    ):
        self._blob_client = blob_client
        self._start = 0
        self._buf = bytearray()
        self._chunks = None

    def writable(self):
        return False

    def read(self, size=-1):
        if self._start == 0:
            self._chunks = self._blob_client.download_blob().chunks()
        while (not size or size < 0) or len(self._buf) < size:
            chunk = next(self._chunks, None)
            if chunk is None:
                break
            self._buf += chunk
            self._start += len(chunk)
        data = self._buf[:size]
        self._buf = self._buf[size:]
        return bytes(data)

    def seek(self, offset):
        if offset != 0:
            raise ValueError("Only seeking to beginning is supported.")
        self._start = offset
        self._buf.clear()
        self._chunks = None


class AzureBlobWriter(io.BufferedIOBase):
    def __init__(
        self,
        blob_client: BlobClient,
        block_size: int = 4 * 1024 * 1024,
    ):
        self._blob_client = blob_client
        self._block_size = block_size
        self._buf = bytearray()
        self._block_offset = 0
        self._blocks = collections.deque([])
        self._closed = False
        self._position = 0

    def readable(self):
        return False

    def write(self, b):
        self._buf += bytes(b)
        while len(self._buf) >= self._block_size:
            block_id = _block_id(self._block_offset)
            self._blob_client.stage_block(
                block_id=block_id, data=bytes(self._buf[: self._block_size])
            )
            self._blocks.append(BlobBlock(block_id))
            self._block_offset += 1
            self._buf = self._buf[self._block_size :]
        self._position += len(b)
        return len(b)

    def flush(self):
        if self._buf:
            block_id = _block_id(self._block_offset)
            self._blob_client.stage_block(block_id=block_id, data=bytes(self._buf))
            self._blocks.append(BlobBlock(block_id))
            self._block_offset += 1
            self._buf.clear()

    def close(self):
        if self._closed:
            return
        self.flush()
        # Put the block list to make the blob.
        self._blob_client.commit_block_list(list(self._blocks))
        self._closed = True
        self._blocks.clear()

    def closed(self):
        return self._closed

    def tell(self):
        return self._position

    def seekable(self):
        return False


class AzureStorage(BlobStorage):
    """Azure storage provider that utilizes the Azure blob storage.

    Args:
        connection_string: See http://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/
            for the connection string format.
        container_name: the name of the blob container in which all blobs
            are stored.

    """

    def __init__(
        self, connection_string, container_name, block_size: int = 4 * 1024 * 1024
    ):
        self._container = ContainerClient.from_connection_string(
            connection_string, container_name
        )
        if not self._container.exists():
            raise ValueError("Container does not exist: " + container_name)
        self._container_name = container_name
        self._block_size = block_size

    def get_object(self, blob_name):
        blob_client = self._container.get_blob_client(blob_name)
        blob_content = blob_client.download_blob().content_as_text()
        return json.loads(blob_content)

    def put_object(self, obj, blob_name):
        blob_client = self._container.get_blob_client(blob_name)
        blob_content = json.dumps(obj).encode("utf-8")
        blob_client.upload_blob(blob_content, overwrite=True)
        return Blob(blob_name, len(blob_content))

    @contextlib.contextmanager
    def get_file(self, blob_name):
        try:
            stream = AzureBlobReader(self._container.get_blob_client(blob_name))
            yield stream
        finally:
            stream.close()

    def put_file(self, fileobj, blob_name):
        self._container.upload_blob(blob_name, fileobj)
        size = fileobj.tell()
        return Blob(blob_name, size)

    def put_avro(self, schema, records, blob_name, codec="snappy"):
        writer = AzureBlobWriter(
            self._container.get_blob_client(blob_name), block_size=self._block_size
        )
        fastavro.writer(writer, schema, records, codec)
        writer.close()
        size = writer.tell()
        return Blob(blob_name, size)

    def put_json(self, records, blob_name, gzip_compress=True):
        writer = AzureBlobWriter(
            self._container.get_blob_client(blob_name), block_size=self._block_size
        )
        newline = "\n"
        if gzip_compress:
            with gzip.open(writer, "wt") as f:
                for record in records:
                    f.write(json.dumps(record))
                    f.write(newline)
        else:
            with io.TextIOWrapper(writer) as f:
                for record in records:
                    f.write(json.dumps(record))
                    f.write(newline)
        writer.close()
        size = writer.tell()
        return Blob(blob_name, size)
