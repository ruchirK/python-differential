from collections import defaultdict
from collection import Collection

class Index:
    def __init__(self):
        self.inner = defaultdict(defaultdict(list))
        self.compaction_frontier = None

    def _validate(self, requested_version):
       assert(self.compaction_frontier is None or requested_version >= self.compaction_frontier)

    # TODO not sure this is exactly the right api.
    def reconstruct_before(self, key, requested_version):
        self._validate(requested_version)
        out = []
        for (version, values) in self.inner[keys].items():
            if version < requested_version:
                out.extend(values)
        return out

    def add_value(self, key, version, value):
        self._validate(version)
        self.inner[key][version].append(value)

    def add_values(self, key, version, values):
        self._validate(version)
        self.inner[key][version].extend(values)

    def compact(self, compaction_version, keys=[]):
        assert(compaction_version)
        def consolidate_values(values):
            consolidated = defaultdict(int)
            for (value, multiplicity) in values:
               consolidated[value] += multiplicity

            return [(value, multiplicity) for (value, multiplicity) in consolidated.items() if multiplicity != 0]
        
        if keys == []:
            keys = [key for key in self.inner.keys()]

        for key in keys:
            versions = self.inner[key]
            to_compact = [version for version in versions.keys() if v <= compaction_version]
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

        for (version, collection) in self.inner:
            collections[version]._extend(collection)

        consolidated = {}
        for (version, collection) in collections.items():
            consolidated[version] = collection.consolidate()

        return CollectionTrace([(version, collection) for (version, collection) in consolidated])


    def join(self, other):
        def join_inner(key, data1, data2):
            out = []
            for (v1, m1) in vals1:
               for (v2, m2)  in vals2:
                  out.append(((key, (data1, data2)), m1 * m2))
            return out
        index_a = Index()
        index_b = Index()
        out = []
        keys_todo = defaultdict(set)

        for (version, collection) in self.inner:
            for ((key, value), multiplicity) in collection.inner:
                index_a.add_value(key, version, (value, multiplicity))
                keys_todo[version].add(key)
        
        for (version, collection) in other.inner:
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
                result.extend(join_inner(delta_a, delta_b))
            result = Collection(result)
            out.append((version, result))
            a.compact(version, keys)
            b.compact(version, keys)
        return CollectionTrace(out)

if __name__ == '__main__':
    a = Collection([(('apple', '$5'), 2), (('banana', '$2'), 1)])
    b = Collection([(('apple', '$3'), 1), (('apple', ('granny smith', '$2')), 1), (('kiwi', '$2'), 1)])
    c = Collection([(('apple', '$5'), 2), (('banana', '$2'), 1), (('apple', '$2'), 20)])
    d = Collection([(('apple', 11), 1), (('apple', 3), 2), (('banana', 2), 3), (('coconut', 3), 1)])
    e = Collection([(1, 1)])

    trace_a = CollectionTrace([(0, a), (1, Collection([(('apple', '$5'), -1), (('apple', '$7'), 1)])), (2, Collection([(('lemon', '$1'), 1)]))])
    print(trace_a.map(lambda data: (data[1], data[0])))
    print(trace_a.filter(lambda data: data[0] != 'apple'))

    trace_b = CollectionTrace([(1, b)])
    print(trace_a.join(trace_b))
