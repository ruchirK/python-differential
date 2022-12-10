from collections import defaultdict

class Collection:
    def __init__(self, dataz=None):
        if dataz is None:
            dataz = []
        self.inner = dataz

    def __repr__(self):
        return f'Collection({self.inner}'

    def concat(self, other):
        out = []
        out.extend(self.inner)
        out.extend(other.inner)
        return Collection(out)

    def negate(self):
        return Collection([(data, -diff) for (data, diff) in self.inner])
    
    def map(self, f):
        return Collection([(f(data), diff) for (data, diff) in self.inner])

    def filter(self, f):
        return Collection([(data, diff) for (data, diff) in self.inner if f(data) == True])

    def consolidate(self):
        consolidated = defaultdict(int)
        for (data, diff) in self.inner:
            consolidated[data] += diff
        consolidated = [(data, diff) for (data, diff) in consolidated.items() if diff != 0]
        consolidated.sort()
        return Collection(consolidated)

    def join(self, other):
        out = []
        for ((k1, v1), d1) in self.inner:
            for ((k2, v2), d2) in other.inner:
                if k1 == k2:
                    out.append(((k1, (v1, v2)), d1 * d2))
        return Collection(out)

    def reduce(self, f):
        keys = defaultdict(list)
        out = []
        for ((key, val), diff) in self.inner:
            keys[key].append((val, diff))
        for (key, vals) in keys.items():
            results = f(vals)
            for (val, diff) in results:
                out.append(((key, val), diff))
        return Collection(out)

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

    def iterate(self, f):
        curr = Collection(self.inner)
        while True:
            result = f(curr)
            if result.inner == curr.inner:
                break
            curr = result
        return curr

    def _extend(self, other):
        self.inner.extend(other.inner)

if __name__ == '__main__':
    a = Collection([(('apple', '$5'), 2), (('banana', '$2'), 1)])
    b = Collection([(('apple', '$3'), 1), (('apple', ('granny smith', '$2')), 1), (('kiwi', '$2'), 1)])
    c = Collection([(('apple', '$5'), 2), (('banana', '$2'), 1), (('apple', '$2'), 20)])
    d = Collection([(('apple', 11), 1), (('apple', 3), 2), (('banana', 2), 3), (('coconut', 3), 1)])
    e = Collection([(1, 1)])

    print(a.concat(b))
    print(a.join(b))
    print(b.join(a))
    print(a.filter(lambda data: data[0] != 'apple'))
    print(a.map(lambda data: (data[1], data[0])))
    print(a.concat(b).count())
    print(a.concat(b).distinct())
    print(c.min())
    print(c.max())
    print(d.sum())

    def add_one(collection):
        return collection.map(lambda data: data + 1)  \
            .concat(collection)                       \
            .filter(lambda data: data <= 5)          \
            .map(lambda data: (data, ()))             \
            .distinct()                               \
            .map(lambda data: data[0])                \
            .consolidate()
    
    collection = Collection([(1, 1)])
    result = collection.iterate(add_one).map(lambda data: (data, data * data))
    print(result)


