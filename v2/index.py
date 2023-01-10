from collections import defaultdict
from collection import Collection


class Index:
    def __init__(self, compaction_frontier=None):
        self._index = defaultdict(list)

    def __repr__(self):
        return "Index({self._index})"

    def add_value(self, key, value):
        """Add a (value, multiplicity) pair for the requested key."""
        self._index[key].append(value)

    def append(self, other):
        """Combine all of the data in other into self."""
        for (key, data) in other._index.items():
            self._index[key].extend(data)

    def get(self, key):
        if key in self._index:
            return self._index[key]
        return []

    def join(self, other):
        """Produce a bounded collection trace containing (key, (val1, val2))
        for all (key, val1) in the first index, and (key, val2) in the second
        index.
        """
        out = []
        for (key, data1) in self._index.items():
            if key not in other._index:
                continue
            data2 = other._index[key]

            for (val1, mul1) in data1:
                for (val2, mul2) in data2:
                    out.append(((key, (val1, val2)), mul1 * mul2))
        return Collection(out)

    def compact(self, keys=[]):
        def consolidate_values(values):
            consolidated = defaultdict(int)
            for (value, multiplicity) in values:
                consolidated[value] += multiplicity

            return [
                (value, multiplicity)
                for (value, multiplicity) in consolidated.items()
                if multiplicity != 0
            ]

        if keys == []:
            keys = [key for key in self._index.keys()]

        for key in keys:
            if key not in self._index:
                continue
            data = self._index.pop(key)
            consolidated = consolidate_values(data)

            if consolidated != []:
                self._index[key].extend(consolidated)
