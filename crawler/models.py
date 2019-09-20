import sys

import numpy as np
import spacy


class _LazySpacyModel:

    def __init__(self, model_name, **kwargs):
        self._model_name = model_name
        self._model = None
        self._model_kwargs = kwargs

    def process(self, text):
        if not self._model:
            self._model = spacy.load(self._model_name, **self._model_kwargs)
        return self._model(text)

    def get_empty_word_vector(self):
        doc = self.process("test")
        return np.zeros(len(doc.vector), dtype=np.float32)


WordVectorModel = _LazySpacyModel("en_vectors_web_lg")

LanguageModel = _LazySpacyModel("en_core_web_sm")
