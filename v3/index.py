"""The implementation of index structures roughly analogous to differential arrangements for manipulating and
accessing (key, value) structured data across multiple versions (times).
"""

from collections import defaultdict
from collection import Collection


class Index:
    """A map from a difference collection trace's keys -> versions at which
    the key has nonzero multiplicity -> (value, multiplicities) that changed.

    Used in operations like join and reduce where the operation needs to
    exploit the key-value structure of the data to run efficiently.

    This implementation is specialized for the case when versions are integers.
    """

    def __init__(self, compaction_frontier=None):
        self._index = defaultdict(lambda: defaultdict(list))
        self.compaction_frontier = compaction_frontier

    def __repr__(self):
        return "Index1D({self._index}, {self.compaction_frontier})"

    def _validate(self, requested_version):
        """Check that requests are at times allowed by the compaction frontier."""
        assert (
            self.compaction_frontier is None
            or requested_version >= self.compaction_frontier
        )

    def reconstruct_at(self, key, requested_version):
        """Produce the accumulated ((key, value), multiplicity) records for the given key, at the requested version."""
        self._validate(requested_version)
        out = []
        for (version, values) in self._index[key].items():
            if version <= requested_version:
                out.extend(values)
        return out

    def add_value(self, key, version, value):
        """Add a (value, multiplicity) pair for the requested key and version."""
        self._validate(version)
        self._index[key][version].append(value)

    def append(self, other):
        """Combine all of the data in other into self."""
        for (key, versions) in other._index.items():
            for (version, data) in versions.items():
                self._index[key][version].extend(data)

    def join(self, other):
        """Produce a bounded collection trace containing (key, (val1, val2))
        for all (key, val1) in the first index, and (key, val2) in the second
        index.

        All outputs are produced at output version = max(version of record 1,
        version of record 2).
        """
        collections = defaultdict(list)
        for (key, versions) in self._index.items():
            if key not in other._index:
                continue
            other_versions = other._index[key]

            for (version1, data1) in versions.items():
                for (version2, data2) in other_versions.items():
                    result_version = max(version1, version2)
                    for (val1, mul1) in data1:
                        for (val2, mul2) in data2:
                            collections[result_version].append(
                                ((key, (val1, val2)), mul1 * mul2)
                            )
        return [
            (version, Collection(c)) for (version, c) in collections.items() if c != []
        ]

    def compact(self, compaction_version, keys=[]):
        """Combine all changes observed before the requested compaction_version
        into the compaction_version.
        """
        self._validate(compaction_version)

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
            versions = self._index[key]
            to_compact = [
                version for version in versions.keys() if version <= compaction_version
            ]
            values = []
            for version in to_compact:
                values.extend(versions.pop(version))

            versions[compaction_version] = consolidate_values(values)
        self.compaction_frontier = compaction_version
