"""The implementation of a bounded, totally ordered sequence of collections
(a bounded collection trace) as a bounded totally ordered sequence of differences.
"""

from collections import defaultdict
from collection import Collection
from index import Index1D as Index


class CollectionTrace:
    """A bounded sequence of collections of data.

    This class represents a difference collection trace, which is to say, the
    collection at version N is logically meant to represent the difference
    between an input collection at version N and an input collection at N - 1.
    This representation is designed for the case where the differences between
    consecutive collections in the input sequence are small, and so storing the
    sequence of differences is both space efficient, and enables efficient
    computation of the sequence of output differences.
    """

    def __init__(self, trace):
        self._trace = trace

    def __repr__(self):
        return f"CollectionTrace({self._trace})"

    def map(self, f):
        """Apply a function to all records in the collection trace."""
        return CollectionTrace(
            [(version, collection.map(f)) for (version, collection) in self._trace]
        )

    def filter(self, f):
        """Filter out records where f(record) evaluates to False from all
        collections in the collection trace.
        """
        return CollectionTrace(
            [(version, collection.filter(f)) for (version, collection) in self._trace]
        )

    def negate(self):
        return CollectionTrace(
            [(version, collection.negate()) for (version, collection) in self._trace]
        )

    def concat(self, other):
        """Concatenate two collection traces together."""
        out = []
        out.extend(self._trace)
        out.extend(other._trace)
        return CollectionTrace(out)

    def consolidate(self):
        """Produce a collection trace where each collection in the trace
        is consolidated.
        """
        collections = defaultdict(Collection)

        for (version, collection) in self._trace:
            collections[version]._extend(collection)

        consolidated = {}
        for (version, collection) in collections.items():
            consolidated[version] = collection.consolidate()

        return CollectionTrace(
            [(version, collection) for (version, collection) in consolidated.items()]
        )

    def join(self, other):
        """Match pairs (k, v1) and (k, v2) from the two input collection
        traces and produce a collection trace containing the corresponding
        (k, (v1, v2)).
        """
        index_a = Index()
        index_b = Index()
        out = []

        for (version, collection) in self._trace:
            for ((key, value), multiplicity) in collection._inner:
                index_a.add_value(key, version, (value, multiplicity))

        for (version, collection) in other._trace:
            for ((key, value), multiplicity) in collection._inner:
                index_b.add_value(key, version, (value, multiplicity))

        # TODO: I believe this implementation actually takes time quadratic in the
        # number of versions which produce nonempty output collections, but it
        # is possible to take time linear in the number of versions.
        for (version, collection) in index_a.join(index_b):
            # Consolidating the output is arguably not necessary, but it makes
            # the outputs easier to read.
            out.append((version, collection.consolidate()))
        return CollectionTrace(out)

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

        for (version, collection) in self._trace:
            for ((key, value), multiplicity) in collection._inner:
                index.add_value(key, version, (value, multiplicity))
                keys_todo[version].add(key)

        versions = [version for version in keys_todo.keys()]
        versions.sort()

        for version in versions:
            keys = keys_todo[version]
            result = []
            for key in keys:
                curr = index.reconstruct_at(key, version)
                curr_out = index_out.reconstruct_at(key, version)
                out = f(curr)
                delta = subtract_values(out, curr_out)
                for (value, multiplicity) in delta:
                    result.append(((key, value), multiplicity))
                    index_out.add_value(key, version, (value, multiplicity))
            output.append((version, Collection(result)))
            index.compact(version, keys)
            index_out.compact(version, keys)

        return CollectionTrace(output)

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

    def interate(self, f):
        """Return the fixpoint of repeatedly applying f to each collection in the trace."""
        curr_in = Collection()
        curr_out = Collection()
        ret = []

        versions = [version in self._trace.keys()]
        versions.sort()

        for version in versions:
            delta = self._trace[version]
            curr = curr_in.concat(delta)
            result = curr.iterate(f)
            delta_out = result.concat(curr_out.negate()).consolidate()
            ret.append((version, delta_out))
            curr_in = curr_in.concat(delta)
            curr_out = curr_out.concat(delta_out)

        return CollectionTrace(ret)


if __name__ == "__main__":
    a = Collection([(("apple", "$5"), 3), (("banana", "$2"), 1)])
    b = Collection([(("apple", "$3"), 1), (("apple", "$2"), 1), (("kiwi", "$2"), 1)])
    c = Collection([(("apple", "$5"), 2), (("banana", "$2"), 1), (("apple", "$2"), 20)])
    d = Collection(
        [(("apple", 11), 1), (("apple", 3), 2), (("banana", 2), 3), (("coconut", 3), 1)]
    )
    e = Collection([(1, 1)])

    trace_a = CollectionTrace(
        [
            (0, a),
            (1, Collection([(("apple", "$5"), -1), (("apple", "$7"), 1)])),
            (2, Collection([(("lemon", "$1"), 1)])),
        ]
    )
    print(trace_a.map(lambda data: (data[1], data[0])))
    print(trace_a.filter(lambda data: data[0] != "apple"))

    trace_b = CollectionTrace(
        [
            (0, b),
            (1, Collection([])),
            (2, Collection([(("lemon", "$22"), 3), (("kiwi", "$1"), 2)])),
        ]
    )
    print(trace_a.join(trace_b))
    print(trace_a.join(trace_b).consolidate())
    print(trace_a.min())
    print(trace_a.max())
    print(trace_a.distinct())
