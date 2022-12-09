from collections import defaultdict
from collection import Collection

class Index:
    def __init__(self):
        self.inner = defaultdict(lambda: defaultdict(list))
        self.compaction_frontier = None

    def _validate(self, requested_version):
       assert(self.compaction_frontier is None or requested_version >= self.compaction_frontier)

    # TODO not sure this is exactly the right api.
    def reconstruct_before(self, key, requested_version):
        self._validate(requested_version)
        out = []
        for (version, values) in self.inner[key].items():
            if version < requested_version:
                out.extend(values)
        return out

    def values(self, key, version):
        return self.inner[key][version]

    def add_value(self, key, version, value):
        self._validate(version)
        self.inner[key][version].append(value)

    def add_values(self, key, version, values):
        self._validate(version)
        self.inner[key][version].extend(values)

    def to_trace(self):
        collections = defaultdict(list)
        for (key, versions) in self.inner.items():
            for (version, data) in versions.items():
                collections[version].extend([((key, val), multiplicity) for (val, multiplicity) in data])
        return CollectionTrace([(version, Collection(collection)) for (version, collection) in collections.items()])

    def compact(self, compaction_version, keys=[]):
        self._validate(compaction_version)
        def consolidate_values(values):
            consolidated = defaultdict(int)
            for (value, multiplicity) in values:
               consolidated[value] += multiplicity

            return [(value, multiplicity) for (value, multiplicity) in consolidated.items() if multiplicity != 0]
        
        if keys == []:
            keys = [key for key in self.inner.keys()]

        for key in keys:
            versions = self.inner[key]
            to_compact = [version for version in versions.keys() if version <= compaction_version]
            values = []
            for version in to_compact:
                values.extend(versions.pop(version))

            versions[compaction_version] = consolidate_values(values)
        self.compaction_frontier = compaction_version

class CollectionTrace:
    def __init__(self, trace):
        self.trace = trace

    def __repr__(self):
        return f'CollectionTrace({self.trace})'

    def map(self, f):
        return CollectionTrace([(version, collection.map(f)) for (version, collection) in self.trace])

    def filter(self, f):
        return CollectionTrace([(version, collection.filter(f)) for (version, collection) in self.trace])

    def concat(self, other):
        out = []
        out.extend(self.trace)
        out.extend(other.trace)
        return CollectionTrace(out)

    def negate(self):
        return CollectionTrace([(version, collection.negate()) for (version, collection) in self.trace])

    def consolidate(self):
        collections = defaultdict(Collection)

        for (version, collection) in self.trace:
            collections[version]._extend(collection)

        consolidated = {}
        for (version, collection) in collections.items():
            print(f"version: {version} collection: {collection}")
            consolidated[version] = collection.consolidate()

        return CollectionTrace([(version, collection) for (version, collection) in consolidated.items()])

    def join(self, other):
        def join_inner(key, data1, data2):
            out = []
            for (v1, m1) in data1:
               for (v2, m2)  in data2:
                  out.append(((key, (v1, v2)), m1 * m2))
            return out
        index_a = Index()
        index_b = Index()
        out = []
        keys_todo = defaultdict(set)

        for (version, collection) in self.trace:
            for ((key, value), multiplicity) in collection.inner:
                index_a.add_value(key, version, (value, multiplicity))
                keys_todo[version].add(key)
        
        for (version, collection) in other.trace:
            for ((key, value), multiplicity) in collection.inner:
                index_b.add_value(key, version, (value, multiplicity))
                keys_todo[version].add(key)

        versions = [version for version in keys_todo.keys()]
        versions.sort()

        for version in versions:
            keys = keys_todo[version]
            result = []
            for key in keys:
                a = index_a.reconstruct_before(key, version)
                b = index_b.reconstruct_before(key, version)
                delta_a = index_a.values(key, version)
                delta_b = index_b.values(key, version)
                result.extend(join_inner(key, a, delta_b))
                result.extend(join_inner(key, delta_a, b))
                result.extend(join_inner(key, delta_a, delta_b))
            result = Collection(result)
            out.append((version, result))
            index_a.compact(version, keys)
            index_b.compact(version, keys)
        return CollectionTrace(out)

    def reduce(self, f):
        # TODO not totally sure about this function vs consolidate_values
        def add_values(first, second):
            result = defaultdict(int)
            for (val, multiplicity) in first:
                result[val] += multiplicity
            for (val, multiplicity) in second:
                result[val] += multiplicity

            return [(val, multiplicity) for (val, multiplicity) in result.items() if multiplicity != 0]
        index = Index()
        index_out = Index()
        keys_todo = defaultdict(set)
        
        for (version, collection) in self.trace:
            for ((key, value), multiplicity) in collection.inner:
                index.add_value(key, version, (value, multiplicity))
                keys_todo[version].add(key)
        
        versions = [version for version in keys_todo.keys()]
        versions.sort()

        for version in versions:
            keys = keys_todo[version]
            result = []
            for key in keys:
                curr = index.reconstruct_before(key, version)
                delta = index.values(key, version)
                vals = add_values(curr, delta)
                result = f(vals)
                curr_output_negated = [(val, -multiplicity) for (val, multiplicity) in index_out.reconstruct_before(key, version)]
                output_delta = add_values(result, curr_output_negated)
                index_out.add_values(key, version, output_delta)
            index.compact(version, keys)

        return index_out.to_trace()

    def count(self):
        def count_inner(vals):
            out = 0
            for (_, diff) in vals:
                out += diff
            return [(out, 1)]

        return self.reduce(count_inner)

    def sum(self):
        def sum_inner(vals):
            out = 0
            for (val, diff) in vals:
                out += (val * diff)
            return [(out, 1)]

        return self.reduce(sum_inner)

    def min(self):
        def min_inner(vals):
            out = vals[0][0]
            for (val, diff) in vals:
                assert(diff > 0)
                if val < out:
                    out = val
            return [(out, 1)]
        return self.reduce(min_inner)

    def max(self):
        def max_inner(vals):
            out = vals[0][0]
            for (val, diff) in vals:
                assert(diff > 0)
                if val > out:
                    out = val
            return [(out, 1)]
        return self.reduce(max_inner)

    def distinct(self):
        def distinct_inner(vals):
            v = set()
            for (val, diff) in vals:
                assert(diff > 0)
                v.add(val)
            return [(val, 1) for val in v]
        return self.reduce(distinct_inner)

    def interate(selfi, f):
        curr_in = Collection()
        curr_out = Collection()
        ret = []

        versions = [version in self.trace.keys()]
        versions.sort()

        for version in versions:
            delta = self.trace[version]
            curr = curr_in.concat(delta)
            result = curr.iterate(f)
            delta_out = result.concat(curr_out.negate()).consolidate()
            ret.append((version, delta_out))
            curr_in = curr_in.concat(delta)
            curr_out = curr_out.concat(delta_out)

        return CollectionTrace(ret)

if __name__ == '__main__':
    a = Collection([(('apple', '$5'), 3), (('banana', '$2'), 1)])
    b = Collection([(('apple', '$3'), 1), (('apple', '$2'), 1), (('kiwi', '$2'), 1)])
    c = Collection([(('apple', '$5'), 2), (('banana', '$2'), 1), (('apple', '$2'), 20)])
    d = Collection([(('apple', 11), 1), (('apple', 3), 2), (('banana', 2), 3), (('coconut', 3), 1)])
    e = Collection([(1, 1)])

    trace_a = CollectionTrace([(0, a), (1, Collection([(('apple', '$5'), -1), (('apple', '$7'), 1)])), (2, Collection([(('lemon', '$1'), 1)]))])
    print(trace_a.map(lambda data: (data[1], data[0])))
    print(trace_a.filter(lambda data: data[0] != 'apple'))

    trace_b = CollectionTrace([(0, b), (1, Collection([])), (2, Collection([(('lemon', '$22'), 3), (('kiwi', '$1'), 2)]))])
    print(trace_a.join(trace_b))
    print(trace_a.join(trace_b).consolidate())
    print(trace_a.min())
    print(trace_a.max())
    print(trace_a.distinct())
