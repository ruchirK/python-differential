"""The implementation of collections (multisets) of data and functional operations over single collections.
"""

from collections import defaultdict


class Collection:
    """A multiset of data"""

    def __init__(self, dataz=None):
        if dataz is None:
            dataz = []
        self._inner = dataz

    def __repr__(self):
        return f"Collection({self._inner})"

    def map(self, f):
        """Apply a function to all records in the collection."""
        return Collection(
            [(f(data), multiplicity) for (data, multiplicity) in self._inner]
        )

    def filter(self, f):
        """Filter out records for which a function f(record) evaluates to False."""
        return Collection(
            [
                (data, multiplicity)
                for (data, multiplicity) in self._inner
                if f(data) == True
            ]
        )

    def negate(self):
        return Collection(
            [(data, -multiplicity) for (data, multiplicity) in self._inner]
        )

    def concat(self, other):
        """Concatenate two collections together."""
        out = []
        out.extend(self._inner)
        out.extend(other._inner)
        return Collection(out)

    def consolidate(self):
        """Produce as output a collection that is logically equivalent to the input
        but which combines identical instances of the same record into one
        (record, multiplicity) pair.
        """
        consolidated = defaultdict(int)
        for (data, multiplicity) in self._inner:
            consolidated[data] += multiplicity
        consolidated = [
            (data, multiplicity)
            for (data, multiplicity) in consolidated.items()
            if multiplicity != 0
        ]
        consolidated.sort()
        return Collection(consolidated)

    def _extend(self, other):
        self._inner.extend(other._inner)
