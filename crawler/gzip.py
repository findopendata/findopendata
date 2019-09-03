import io
import gzip

class Bytes2GzipStreamReader(io.IOBase):
    """This classes is a wrapper for gzipping a byte stream."""

    def __init__(self, byte_stream, chunk_size=1024*1024*5):
        """Initialize the gzip wrapper given the input byte stream.

        Args:
            byte_stream: an object that implements io.RawIOBase.
            chunk_size (optional): the chunk size used to read the input.
        """
        self._byte_stream = byte_stream
        self._chunk_size = chunk_size
        self._buffer = io.BytesIO()
        self._gz = gzip.GzipFile(fileobj=self._buffer, mode="wb")
        self._position = 0

    def read(self, size=-1):
        """Reads at most size number of bytes from the gzip compressed byte
        stream.
        """
        while size < 0 or len(self._buffer.getbuffer()) < size:
            data = self._byte_stream.read(self._chunk_size)
            if len(data) == 0:
                self._gz.close()
                break
            self._gz.write(data)
            self._gz.flush()
        self._buffer.seek(0)
        compressed = self._buffer.read(size)
        remain = self._buffer.read(-1)
        self._buffer.seek(0)
        self._buffer.write(remain)
        self._buffer.truncate()
        self._position += len(compressed)
        return compressed

    def tell(self):
        """Returns the current byte position in this gzip compressed stream.
        """
        return self._position

    def _reset(self):
        self._byte_stream.seek(0)
        self.__init__(self._byte_stream, chunk_size=self._chunk_size)

    def seek(self, offset, whence=io.SEEK_SET):
        """Moves the gzip compressed output byte stream to a given byte offset.
        """
        if whence != io.SEEK_SET:
            raise io.UnsupportedOperation("non-rewinding seek is not supported")
        self._reset()
        self.read(offset)
        return self._position

