import unittest
import io
import gzip
import random
import string
from crawler.gzip import Bytes2GzipStreamReader

class TestGzipStreamIO(unittest.TestCase):

    def test_basic(self):
        source_data = "".join(random.choice(string.ascii_letters) for _ in range(1024)).encode("utf-8")*1024*15
        print("Source data size: ", len(source_data))
        source_file = io.BytesIO(source_data)
        w = Bytes2GzipStreamReader(source_file)
        compressed_file = io.BytesIO()
        while True:
            data = w.read(1024*5)
            if len(data) == 0:
                break
            compressed_file.write(data)
        compressed_file.flush()

        # Verify correctness
        compressed_data = compressed_file.getvalue()
        uncompressed_data = gzip.decompress(compressed_data)
        self.assertEqual(source_data, uncompressed_data)

        # Check compression ratio
        print("Compression ratio: ", len(compressed_data) / len(source_data))
