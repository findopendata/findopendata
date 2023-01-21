from . import _SpacyModel

# When _LazySpacyModel is used, importing the model won't immediately load
# the model, however every Celery worker process will load a redundant copy of
# the model into its own memory -- high memory usage.
# WordVectorModel = _LazySpacyModel("en_vectors_web_lg")

# When _SpacyModel is used, the parent Celery process will load the model
# into its memory, and Celery workers (forked child processes) may not need
# to have physical redundant copies due to "copy-on-write".
WordVectorModel = _SpacyModel("en_core_web_sm")
