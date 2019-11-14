from collections import OrderedDict

from .column_sketch import ColumnSketch


class TableSketch:
    """A TableSketch contains summaries of a table, including the column 
    sketches.

    Args:
        record_sample_size: the number of record to include in the sample.
        column_sketch_kwargs: keyword arguments for ColumnSketch's constructor.
    """

    def __init__(self, record_sample_size=20, **column_sketch_kwargs):
        self._column_sketches = {}
        self._record_sample_size = record_sample_size
        self._sample = []
        self._column_names = []
        self._column_sketch_kwargs = column_sketch_kwargs
    
    @property
    def column_sketches(self):
        """Column sketches in the order of column names."""
        return [self._column_sketches[name] for name in self._column_names]
    
    @property
    def record_sample(self):
        """Record samples as a list of dict."""
        return self._sample
    
    @property
    def column_names(self):
        """Column names in the order from left to right."""
        return self._column_names
    
    def update(self, record):
        # Check type.
        if not isinstance(record, OrderedDict):
            raise TypeError("record must be an OrderedDict")
        # Assign column names.
        if not self._column_names:
            self._column_names = list(record.keys())
        # Update column sketches.
        for column_name, value in record.items():
            if column_name not in self._column_sketches:
                self._column_sketches[column_name] = ColumnSketch(column_name, 
                        **self._column_sketch_kwargs)
            self._column_sketches[column_name].update(value)
        # Update record sample.
        if len(self._sample) < self._record_sample_size:
            self._sample.append(dict(record))
