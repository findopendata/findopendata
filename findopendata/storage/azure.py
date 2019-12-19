import io
import base64
import contextlib
import urllib
import collections
import gzip

import fastavro
import simplejson as json
from azure.storage.blob import BlockBlobService, BlobBlock

from .base import Blob, BlobStorage


def _encode_base64(data):
    if isinstance(data, str):
        data = data.encode('utf-8')
    encoded = base64.b64encode(data)
    return encoded.decode('utf-8')


def _block_id(offset):
   return urllib.parse.quote(_encode_base64('{0:032d}'.format(offset)))


class AzureBlobReader(io.BufferedIOBase):
    
    def __init__(self, block_blob_service: BlockBlobService, 
            container_name: str, blob_name: str):
        self._service = block_blob_service
        self._container_name = container_name
        self._blob_name = blob_name
        self._start = 0
        self._chunk_size = block_blob_service.MAX_CHUNK_GET_SIZE 
        self._buf = bytearray()
        # Get the total size of the blob.
        blob = self._service.get_blob_properties(self._container_name, 
                self._blob_name)
        self._total_size = blob.properties.content_length
    
    def writable(self):
        return False
    
    def read(self, size=-1):
        while (not size or size < 0) or len(self._buf) < size:
            if self._start >= self._total_size:
                break
            end_range = self._start + self._chunk_size - 1
            if end_range >= self._total_size:
                end_range = self._total_size - 1
            blob = self._service.get_blob_to_bytes(self._container_name, 
                    self._blob_name, start_range=self._start, 
                    end_range=end_range)
            if len(blob.content) == 0:
                break
            self._buf += bytearray(blob.content)
            self._start += self._chunk_size
        data = self._buf[:size]
        self._buf = self._buf[size:]
        return bytes(data)
    
    def seek(self, offset):
        self._start = offset
        self._buf.clear()


class AzureBlobWriter(io.BufferedIOBase):

    def __init__(self, block_blob_service: BlockBlobService, 
            container_name: str, blob_name: str):
        self._service = block_blob_service
        self._container_name = container_name
        self._blob_name = blob_name
        self._block_size = block_blob_service.MAX_BLOCK_SIZE
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
            self._service.put_block(self._container_name, self._blob_name,
                    bytes(self._buf[:self._block_size]), block_id)
            self._blocks.append(BlobBlock(block_id))
            self._block_offset += 1
            self._buf = self._buf[self._block_size:]
        self._position += len(b)
        return len(b)

    def flush(self):
        if self._buf:
            block_id = _block_id(self._block_offset)
            self._service.put_block(self._container_name, self._blob_name,
                    bytes(self._buf), block_id)
            self._blocks.append(BlobBlock(block_id))
            self._block_offset += 1
            self._buf.clear()
    
    def close(self):
        if self._closed:
            return
        self.flush()
        # Put the block list to make the blob.
        self._service.put_block_list(self._container_name, self._blob_name,
                list(self._blocks))
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
    def __init__(self, connection_string, container_name):
        self._service = BlockBlobService(connection_string=connection_string)
        if not self._service.exists(container_name):
            raise ValueError("Container does not exist: "+container_name)
        self._container_name = container_name
    
    def get_object(self, blob_name):
        blob = self._service.get_blob_to_text(self._container_name,
                blob_name)
        return json.loads(blob.content) 
    
    def put_object(self, obj, blob_name):
        data = json.dumps(obj).encode("utf-8")
        self._service.create_blob_from_bytes(self._container_name, blob_name,
                data)
        return Blob(blob_name, len(data))

    @contextlib.contextmanager
    def get_file(self, blob_name):
        try:
            stream = AzureBlobReader(self._service, 
                    self._container_name, blob_name)
            yield stream
        finally:
            stream.close()

    def put_file(self, fileobj, blob_name):
        self._service.create_blob_from_stream(self._container_name, blob_name,
                fileobj)
        size = fileobj.tell()
        return Blob(blob_name, size)
    
    def put_avro(self, schema, records, blob_name, codec='snappy'):
        writer = AzureBlobWriter(self._service,
            self._container_name, blob_name)
        fastavro.writer(writer, schema, records, codec)
        writer.close()
        size = writer.tell() 
        return Blob(blob_name, size)

    def put_json(self, records, blob_name, gzip_compress=True):
        writer = AzureBlobWriter(self._service,
            self._container_name, blob_name)
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