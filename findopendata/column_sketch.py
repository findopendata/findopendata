import farmhash
import simplejson as json
import numpy as np
from datasketch import MinHash, HyperLogLogPlusPlus

from .models.word_vector_models import WordVectorModel


def _is_number(x):
    try:
        a = float(x)
    except ValueError:
        return False
    else:
        return True


class ColumnSketch:
    """A Column Sketch contains a summary of a table column. 

    Args:
        column_name: the extracted column name.
        minhash_size: the number of permutations to use for MinHash.
        minhash_seed: the random seed used by MinHash.
        hyperloglog_p: the precision parameter used by HyperLogLog.
        sample_size: the size of sample to be kept.
        enable_word_vector_data: whether to build word embedding vector for 
            data values -- can be 10x more expensive.
    """
    def __init__(self, column_name, 
        minhash_size=256, 
        minhash_seed=43, 
        hyperloglog_p=8,
        sample_size=100,
        enable_word_vector_data=False,
        model=WordVectorModel,
        ):
        self._column_name = column_name
        self._sample = set([])
        self._sample_size = sample_size
        self._count = 0
        self._empty_count = 0
        self._oov_count = 0
        self._numeric_count = 0
        self._minhash = MinHash(num_perm=minhash_size, seed=minhash_seed,
                hashfunc=self._hashfunc32)
        self._hhl = HyperLogLogPlusPlus(p=hyperloglog_p,
                hashfunc=self._hashfunc64) 
        self._enabled_word_vec_data = enable_word_vector_data
        self._model = model
        self._sum_vector = self._model.get_empty_word_vector()

    def _hashfunc32(self, str_value):
        return farmhash.hash32(str_value)
    
    def _hashfunc64(self, str_value):
        return farmhash.hash64(str_value)
    
    @property
    def column_name(self):
        """The extracted column name.
        """
        return self._column_name

    @property
    def sample(self):
        """A sample (non-random) of the data values in the column as a list.
        """
        return list(self._sample)
    
    @property
    def count(self):
        """The total number of data values (i.e. rows) including
        the empty ones.
        """
        return self._count
    
    @property
    def empty_count(self):
        """The number of empty data values.
        """
        return self._empty_count
    
    @property
    def non_empty_count(self):
        """The number of non-empty data values.
        """
        return self._count - self._empty_count
    
    @property
    def out_of_vocabulary_count(self):
        """The number of data values that are non-empty and outside of
        the language model's vocabulary.
        """
        return self._oov_count
    
    @property
    def in_vocabulary_count(self):
        """The number of data values that are non-empty and in
        the language model's vocabulary.
        """
        return self._count - self._empty_count - self._oov_count
    
    @property
    def numeric_count(self):
        """The number of data values that are non-empty and numerical.
        """
        return self._numeric_count
    
    @property
    def is_numeric(self):
        """Whether the column is numeric, based on if at least 50% of rows
        are numeric.
        """
        if self.non_empty_count == 0:
            return False
        return (float(self._numeric_count) / float(self.non_empty_count)) >= 0.5
    
    @property
    def distinct_count(self):
        """The approximate distinct count made by the HyperLogLog.
        """
        if len(self._sample) < self._sample_size:
            return len(self._sample)
        return max(len(self._sample), self._hhl.count())
    
    @property
    def word_vector_column_name(self):
        """The word embedding vector of the column name as a list.
        """
        doc = self._model.process(self.column_name)
        vectors = [token.vector for token in doc if token.has_vector]
        if len(vectors) == 0:
            return None
        return list(float(v) for v in np.sum(vectors, axis=0))
    
    @property
    def word_vector_data(self):
        """The mean word embedding vector of all data values as a list.
        """
        if not self._enabled_word_vec_data:
            return None
        if self.in_vocabulary_count == 0:
            return None
        vector = self._sum_vector / np.float32(self.in_vocabulary_count)
        return list(float(v) for v in vector)
    
    @property
    def minhash(self):
        """The hash values in the MinHash.
        """
        return list(int(v) for v in self._minhash.digest())

    @property
    def seed(self):
        """The random seed used for MinHash.
        """
        return self._minhash.seed
    
    @property
    def hyperloglog(self):
        """The register values of the HyperLogLog counter.
        """
        return list(int(v) for v in self._hhl.digest())

    def update(self, value):
        """Add a data value into the sketch.
        """
        # Update counter.
        self._count += 1
        if not isinstance(value, str):
            value = json.dumps(value, sort_keys=True)
        # Clean the value
        value = value.strip().lower()
        # Skip if the value is empty string.
        if len(value) == 0:
            self._empty_count += 1
            return
        if _is_number(value):
            self._numeric_count += 1
        # Add to sample.
        if len(self._sample) < self._sample_size:
            self._sample.add(value)
        # Update the MinHash sketch.
        self._minhash.update(value)
        # Update the HyperLogLog sketch.
        self._hhl.update(value)
        # Skip word vector extraction if not enabled.
        if not self._enabled_word_vec_data:
            return
        # Update the sum of word embeddings.
        vectors = [token.vector for token in self._model.process(value) 
                if token.has_vector]
        if len(vectors) > 0:
            self._sum_vector += np.sum(vectors, axis=0)
        else:
            self._oov_count += 1

