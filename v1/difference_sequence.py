"""The implementation of a collection that changes as a sequence of difference
collections describing each change.
"""

from collections import defaultdict
from collection import Collection
from index import Index
from itertools import zip_longest


class DifferenceSequence:
    """A collection that goes through a sequence of changes.

    Each change to the collection is described in a difference collection that
    describes the change between the current version of the collection and the
    previous version.

    This representation is designed for the case where the differences between
    consecutive versions in the sequence are small, and so storing the
    sequence of differences is both space efficient, and enables efficient
    computation of the sequence of output differences.
    """

    def __init__(self, trace):
        self._inner = trace

    def __repr__(self):
        return f"DifferenceSequence({self._inner})"

    def map(self, f):
        """Apply a function to all records in the collection trace."""
        return DifferenceSequence([collection.map(f) for collection in self._inner])

    def filter(self, f):
        """Filter out records where f(record) evaluates to False from all
        collections in the collection trace.
        """
        return DifferenceSequence([collection.filter(f) for collection in self._inner])

    def negate(self):
        return DifferenceSequence([collection.negate() for collection in self._inner])

    def concat(self, other):
        """Concatenate two collection traces together."""
        inputs = zip_longest(self._inner, other._inner, fillvalue=Collection())
        return DifferenceSequence([a.concat(b) for (a, b) in inputs])

    def consolidate(self):
        """Produce a collection trace where each collection in the trace
        is consolidated.
        """
        out = []
        for collection in self._inner:
            out.append(collection.consolidate())

        return DifferenceSequence(out)

    def join(self, other):
        """Match pairs (k, v1) and (k, v2) from the two input collection
        traces and produce a collection trace containing the corresponding
        (k, (v1, v2)).
        """
        index_a = Index()
        index_b = Index()
        out = []

        for (collection_a, collection_b) in zip_longest(
            self._inner, other._inner, fillvalue=Collection()
        ):
            delta_a = Index()
            delta_b = Index()
            result = Collection()

            for ((key, value), multiplicity) in collection_a._inner:
                delta_a.add_value(key, (value, multiplicity))
            for ((key, value), multiplicity) in collection_b._inner:
                delta_b.add_value(key, (value, multiplicity))

            result._extend(delta_a.join(index_b))
            index_a.append(delta_a)
            result._extend(index_a.join(delta_b))
            index_b.append(delta_b)
            # Consolidating the output is not strictly necessary and is only done here to make the output easier to inspect visually.
            out.append(result.consolidate())
        return DifferenceSequence(out)

    def reduce(self, f):
        """Apply a reduction function to all record values, grouped by key."""

        def subtract_values(first, second):
            result = defaultdict(int)
            for (v1, m1) in first:
                result[v1] += m1
            for (v2, m2) in second:
                result[v2] -= m2

            return [
                (val, multiplicity)
                for (val, multiplicity) in result.items()
                if multiplicity != 0
            ]

        index = Index()
        index_out = Index()
        keys_todo = defaultdict(set)
        output = []

        for collection in self._inner:
            keys_todo = set()
            result = []
            for ((key, value), multiplicity) in collection._inner:
                index.add_value(key, (value, multiplicity))
                keys_todo.add(key)

            keys = [key for key in keys_todo]
            for key in keys:
                curr = index.get(key)
                curr_out = index_out.get(key)
                out = f(curr)
                delta = subtract_values(out, curr_out)
                for (value, multiplicity) in delta:
                    result.append(((key, value), multiplicity))
                    index_out.add_value(key, (value, multiplicity))
            output.append(Collection(result))
            index.compact(keys)
            index_out.compact(keys)

        return DifferenceSequence(output)

    def count(self):
        """Count the number of times each key occurs in each collection in the collection
        trace.
        """

        def count_inner(vals):
            out = 0
            for (_, diff) in vals:
                out += diff
            return [(out, 1)]

        return self.reduce(count_inner)

    def sum(self):
        """Produce the sum of all the values paired with each key, for each
        collection in the trace.
        """

        def sum_inner(vals):
            out = 0
            for (val, diff) in vals:
                out += val * diff
            return [(out, 1)]

        return self.reduce(sum_inner)

    def min(self):
        """Produce the minimum value associated with each key, for each collection in
        the trace.
        """

        def min_inner(vals):
            consolidated = defaultdict(int)
            for (val, multiplicity) in vals:
                consolidated[val] += multiplicity
            vals = [
                (val, multiplicity)
                for (val, multiplicity) in consolidated.items()
                if multiplicity != 0
            ]
            if len(vals) != 0:
                out = vals[0][0]
                for (val, multiplicity) in vals:
                    assert multiplicity > 0
                    if val < out:
                        out = val
                return [(out, 1)]
            else:
                return []

        return self.reduce(min_inner)

    def max(self):
        """Produce the minimum value associated with each key, for each collection in
        the trace.
        """

        def max_inner(vals):
            consolidated = defaultdict(int)
            for (val, multiplicity) in vals:
                consolidated[val] += multiplicity
            vals = [
                (val, multiplicity)
                for (val, multiplicity) in consolidated.items()
                if multiplicity != 0
            ]
            if len(vals) != 0:
                out = vals[0][0]
                for (val, multiplicity) in vals:
                    assert multiplicity > 0
                    if val > out:
                        out = val
                return [(out, 1)]
            else:
                return []

        return self.reduce(max_inner)

    def distinct(self):
        def distinct_inner(vals):
            consolidated = defaultdict(int)
            for (val, multiplicity) in vals:
                consolidated[val] += multiplicity
            vals = [
                (val, multiplicity)
                for (val, multiplicity) in consolidated.items()
                if multiplicity != 0
            ]
            for (val, multiplicity) in vals:
                assert multiplicity > 0
            return [(val, 1) for (val, _) in vals]

        return self.reduce(distinct_inner)

    def iterate(self, f):
        """Return the fixpoint of repeatedly applying f to each collection in the trace."""
        # TODO


if __name__ == "__main__":
    a = Collection([(("apple", "$5"), 3), (("banana", "$2"), 1)])
    b = Collection([(("apple", "$3"), 1), (("apple", "$2"), 1), (("kiwi", "$2"), 1)])
    c = Collection([(("apple", "$5"), 2), (("banana", "$2"), 1), (("apple", "$2"), 20)])
    d = Collection(
        [(("apple", 11), 1), (("apple", 3), 2), (("banana", 2), 3), (("coconut", 3), 1)]
    )
    e = Collection([(1, 1)])

    trace_a = DifferenceSequence(
        [
            a,
            Collection([(("apple", "$5"), -1), (("apple", "$7"), 1)]),
            Collection([(("lemon", "$1"), 1)]),
        ]
    )
    print(trace_a.map(lambda data: (data[1], data[0])))
    print(trace_a.filter(lambda data: data[0] != "apple"))

    trace_b = DifferenceSequence(
        [
            b,
            Collection([]),
            Collection([(("lemon", "$22"), 3), (("kiwi", "$1"), 2)]),
        ]
    )
    print(trace_a.join(trace_b))
    print(trace_a.join(trace_b).consolidate())
    print(trace_a.min())
    print(trace_a.max())
    print(trace_a.distinct())
