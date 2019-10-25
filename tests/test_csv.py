import unittest
import io

from crawler.parsers.csv import csv2json

TEST_CSV_1 = """Column1,Column2,Column3
1,2,3
1,3,4
2,4,5
"""

TEST_CSV_2 = """Title,,
ColumnX,ColumnY,ColumnZ
4,1,3
4,1,4
6,34,123
"""

TEST_CSV_3 = """4,1,3
4,1,4
6,34,123
6,34,123
6,34,123
6,34,123
6,34,123
6,34,123
6,34,123
6,34,123
6,34,123
6,34,123
6,34,123
6,34,123
"""

class TestCSV2JSON(unittest.TestCase):

    def test_csv2json_utf8(self):
        f = io.BytesIO(TEST_CSV_1.encode("utf-8"))
        records = list(csv2json(f))
        self.assertEqual(records[0], {"Column1": "1", "Column2": "2", "Column3": "3"})
        self.assertEqual(records[1], {"Column1": "1", "Column2": "3", "Column3": "4"})
        self.assertEqual(records[2], {"Column1": "2", "Column2": "4", "Column3": "5"})

    def test_csv2json_iso_8859_1(self):
        f = io.BytesIO(TEST_CSV_1.encode("iso-8859-1"))
        records = list(csv2json(f))
        self.assertEqual(records[0], {"Column1": "1", "Column2": "2", "Column3": "3"})
        self.assertEqual(records[1], {"Column1": "1", "Column2": "3", "Column3": "4"})
        self.assertEqual(records[2], {"Column1": "2", "Column2": "4", "Column3": "5"})

    def test_csv2json_header_detect(self):
        f = io.BytesIO(TEST_CSV_2.encode("utf-8"))
        records = list(csv2json(f))
        self.assertEqual(records[0], {"ColumnX": "4", "ColumnY": "1", "ColumnZ": "3"})
        self.assertEqual(records[1], {"ColumnX": "4", "ColumnY": "1", "ColumnZ": "4"})
        self.assertEqual(records[2], {"ColumnX": "6", "ColumnY": "34", "ColumnZ": "123"})

        f = io.BytesIO(TEST_CSV_3.encode("iso-8859-1"))
        records = list(csv2json(f, allow_no_header=True, header_prefix="C"))
        self.assertEqual(records[0], {"C0": "4", "C1": "1", "C2": "3"})


if __name__ == "__main__":
    unittest.main()

