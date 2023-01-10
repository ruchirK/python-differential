"""The implementation of totally ordered, multidimensional versions (times) for use within a differential dataflow.
"""


class Version:
    """A totally ordered version (time), consisting of a tuple of
    integers, ordered lexicographically.

    All versions within a scope of a dataflow must have the same dimension/number
    of coordinates.
    """

    def __init__(self, version):
        if isinstance(version, int):
            assert version >= 0
            self.inner = (version,)
        elif isinstance(version, list) or isinstance(version, tuple):
            for i in version:
                assert isinstance(i, int)
                assert i >= 0
            self.inner = tuple(version)
        else:
            assert 0 > 1

    def __repr__(self):
        return f"Version({self.inner})"

    def __eq__(self, other):
        return self.inner == other.inner

    def __lt__(self, other):
        return self.inner.__lt__(other.inner)

    def __le__(self, other):
        return self.__lt__(other) or self.__eq__(other)

    def __hash__(self):
        return hash(self.inner)

    def _validate(self, other):
        assert len(self.inner) > 0
        assert len(self.inner) == len(other.inner)

    def extend(self):
        elements = [e for e in self.inner]
        elements.append(0)
        return Version(elements)

    def truncate(self):
        elements = [e for e in self.inner]
        elements.pop()
        return Version(elements)

    def apply_step(self, step, max_value):
        assert step > 0
        assert len(self.inner) > 1
        elements = [e for e in self.inner]

        pos = 1
        while True:
            if elements[-pos] < max_value or pos == len(elements):
                elements[-pos] += step
                break
            else:
                elements[-pos] = 0
                pos += 1
        output = Version(elements)
        assert output > self
        return output
