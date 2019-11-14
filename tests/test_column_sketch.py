import unittest
import random
import cProfile

from findopendata.column_sketch import ColumnSketch
from findopendata.models.word_vector_models import WordVectorModel as lm


WORDS = ["english", "french", "spanish", "russian", "italian", "arabic", 
        "sanskrit", "chinese", "japanese", "korean"]

TEST_COLUMN_1_NAME = "Countries"
TEST_COLUMN_1 = WORDS * 10000


class TestColumnSketch(unittest.TestCase):

    def setUp(self):
        self.pr = cProfile.Profile()
        self.pr.enable()
    
    def tearDown(self):
        self.pr.disable()
        self.pr.dump_stats("{}.prof".format(self._testMethodName))

    def test_disable_word_vec_data(self):
        sketch = ColumnSketch(TEST_COLUMN_1_NAME, 
                minhash_size=256, 
                minhash_seed=43, 
                hyperloglog_p=8,
                sample_size=100, 
                enable_word_vector_data=False,
                model=lm,
                )
        for word in TEST_COLUMN_1:
            sketch.update(word)
        self.assertTrue(sketch.count == len(TEST_COLUMN_1))
        self.assertTrue(len(sketch.sample) == len(WORDS))
        self.assertTrue(sketch.word_vector_column_name is not None)
        self.assertTrue(sketch.word_vector_data is None)
        self.assertTrue(sketch.non_empty_count == len(TEST_COLUMN_1))
        self.assertTrue(sketch.hyperloglog is not None)
        self.assertTrue(len(sketch.minhash) == 256)

    def test_enable_word_vec_data(self):
        sketch = ColumnSketch(TEST_COLUMN_1_NAME, 
                minhash_size=256, 
                minhash_seed=43, 
                hyperloglog_p=8,
                sample_size=100, 
                enable_word_vector_data=True,
                model=lm,
                )
        for word in TEST_COLUMN_1:
            sketch.update(word)
        self.assertTrue(sketch.count == len(TEST_COLUMN_1))
        self.assertTrue(len(sketch.sample) == len(WORDS))
        self.assertTrue(sketch.word_vector_column_name is not None)
        self.assertTrue(sketch.word_vector_data is not None)
        self.assertTrue(sketch.non_empty_count == len(TEST_COLUMN_1))
        self.assertTrue(sketch.hyperloglog is not None)
        self.assertTrue(len(sketch.minhash) == 256)


if __name__ == "__main__":
    unittest.main()