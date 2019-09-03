import unittest
import gzip

from crawler.util import get_mime_buffer

class TestUtil(unittest.TestCase):

    def test_get_mine_buffer_plain(self):
        data = "abcdefg中文".encode("utf-8")
        mime_type, encoding = get_mime_buffer(data)
        self.assertEqual(encoding, "utf-8")
        self.assertEqual(mime_type, "text/plain")

    def test_get_mine_buffer_html(self):
        data = "<html><body>abcdefg中文</body></html>".encode("utf-8")
        mime_type, encoding = get_mime_buffer(data)
        self.assertEqual(encoding, "utf-8")
        self.assertEqual(mime_type, "text/html")

    def test_get_mine_buffer_iso_8859_1(self):
        data = "abcdefgé".encode("iso-8859-1")
        mime_type, encoding = get_mime_buffer(data)
        self.assertEqual(encoding, "iso-8859-1")
        self.assertEqual(mime_type, "text/plain")


if __name__ == "__main__":
    unittest.main()
