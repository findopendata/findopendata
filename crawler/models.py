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


class _SpacyModel(_LazySpacyModel):
    
    def __init__(self, model_name, **kwargs):
        super().__init__(model_name, **kwargs)
        self._model = spacy.load(self._model_name, **self._model_kwargs)


LanguageModel = _LazySpacyModel("en_core_web_sm")

# When _LazySpacyModel is used, importing the model won't immediately load
# the model, however every Celery worker process will load a redundant copy of
# the model into its own memory -- high memory usage.
# WordVectorModel = _LazySpacyModel("en_vectors_web_lg")

# When _SpacyModel is used, the parent Celery process will load the model 
# into its memory, and Celery workers (forked child processes) may not need
# to have physical redundant copies due to "copy-on-write".
WordVectorModel = _SpacyModel("en_vectors_web_lg")

