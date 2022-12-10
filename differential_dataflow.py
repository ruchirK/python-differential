from collections import defaultdict, deque

from collection import Collection
from differential_dataflow_1d import MessageType, CollectionStreamListener

class Index:
    def __init__(self):
        self.inner = defaultdict(lambda: defaultdict(list))
        # TODO: take an initial time?
        self.compaction_frontier = None

    def _validate(self, requested_version):
       assert(self.compaction_frontier is None or self.compaction_frontier.less_equal(requested_version))

    def reconstruct_at(self, key, requested_version):
        self._validate(requested_version)
        out = []
        for (version, values) in self.inner[key].items():
            if version.less_equal(requested_version):
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

    def append(self, other):
        for (key, versions) in other.inner.items():
            for (version, data) in versions.items():
                self.inner[key][version].extend(data)

    def to_trace(self):
        collections = defaultdict(list)
        for (key, versions) in self.inner.items():
            for (version, data) in versions.items():
                collections[version].extend([((key, val), multiplicity) for (val, multiplicity) in data])
        return CollectionTrace([(version, Collection(collection)) for (version, collection) in collections.items()])

    def join(self, other):
        collections = defaultdict(list)
        for (key, versions) in self.inner.items():
            if key not in other.inner:
                continue
            other_versions = other.inner[key]

            for (version1, data1) in versions.items():
                for (version2, data2) in other_versions.items():
                    for (val1, mul1) in data1:
                        for (val2, mul2) in data2:
                            result_version = version1.join(version2)
                            collections[result_version].append(((key, (val1, val2)), mul1 * mul2))
        return [(version, Collection(c)) for (version, c) in collections.items() if c != []]

    def compact(self, compaction_frontier, keys=[]):
        self._validate(compaction_frontier)
        def consolidate_values(values):
            consolidated = defaultdict(int)
            for (value, multiplicity) in values:
               consolidated[value] += multiplicity

            return [(value, multiplicity) for (value, multiplicity) in consolidated.items() if multiplicity != 0]
        
        if keys == []:
            keys = [key for key in self.inner.keys()]

        for key in keys:
            versions = self.inner[key]
            to_compact = [version for version in versions.keys() if compaction_frontier.less_equal_version(version) is not True]
            to_consolidate = set()
            for version in to_compact:
                values = versions.pop(version)
                new_version = version.advance_by(compaction_frontier)
                versions[new_version].extend(values)
                to_consolidate.add(new_version)
            for version in to_consolidate:
                values = versions.pop(version)
                versions[version] = consolidate_values(values)
        self.compaction_frontier = compaction_frontier

class Graph:
    def __init__(self, initial_frontier):
        self.streams = []
        self.operators = []
        self.frontier_stack = [initial_frontier]

    def new_stream(self):
        input_stream = CollectionStream(self)
        self.streams.append(input_stream)
        return input_stream

    def add_operator(self, operator):
        self.operators.append(operator)

    def frontier(self):
        return self.frontier_stack[-1]

    def push_frontier(self, new_frontier):
        self.frontier_stack.append(new_frontier)

    def pop_frontier(self):
        self.frontier_stack.pop()

    def step(self):
       for op in self.operators:
           op.run()
class CollectionStream:
    def __init__(self, graph):
        self.queues = []
        self.graph = graph

    def send_data(self, version, collection):
        assert(len(self.queues) > 0)
        for q in self.queues:
            q.appendleft((MessageType.DATA, version, collection))

    def send_frontier(self, frontier):
        assert(len(self.queues) > 0)
        for q in self.queues:
            q.appendleft((MessageType.FRONTIER, frontier, []))

    def connect_to(self):
        q = deque()
        self.queues.append(q)
        return CollectionStreamListener(q)

    def map(self, f):
        output = self.graph.new_stream()
        map_operator = MapOperator(self.connect_to(), output, f, self.graph.frontier())
        self.graph.add_operator(map_operator)
        return output
    
    def filter(self, f):
        output = self.graph.new_stream()
        filter_operator = FilterOperator(self.connect_to(), output, f, self.graph.frontier())
        self.graph.add_operator(filter_operator)
        return output
    
    def negate(self, ):
        output = self.graph.new_stream()
        negate_operator = NegateOperator(self.connect_to(), output, self.graph.frontier())
        self.graph.add_operator(negate_operator)
        return output
    
    def concat(self, other):
        # TODO check that these are edges on the same graph
        output = self.graph.new_stream()
        concat_operator = ConcatOperator(self.connect_to(), other.connect_to(), output, self.graph.frontier())
        self.graph.add_operator(concat_operator)
        return output
    
    def join(self, other):
        # TODO check that these are edges on the same graph
        output = self.graph.new_stream()
        join_operator = JoinOperator(self.connect_to(), other.connect_to(), output, self.graph.frontier())
        self.graph.add_operator(join_operator)
        return output

class Version:
    def __init__(self, version):
        if isinstance(version, int):
            assert(version >= 0)
            self.inner = (version,)
        elif isinstance(version, list) or isinstance(version, tuple):
            for i in version:
                assert(isinstance(i, int))
                assert(i >= 0)
            self.inner = tuple(version)
        else:
           assert(0 > 1)

    def __repr__(self):
        return f'Version({self.inner})'

    def __eq__(self, other):
        return self.inner == other.inner
    
    def __hash__(self):
        return hash(self.inner)

    def _validate(self, other):
        assert(len(self.inner) > 0)
        assert(len(self.inner) == len(other.inner))

    def less_equal(self, other):
        self._validate(other)

        for (i1, i2) in zip(self.inner, other.inner):
            if i1 > i2:
                return False
        return True
    
    def less_than(self, other):
        if self.less_equal(other) is True and self.inner != other.inner:
            return True
        return False

    def join(self, other):
        self._validate(other)
        out = []

        for (i1, i2) in zip(self.inner, other.inner):
            out.append(max(i1, i2))
        return Version(out)

    def meet(self, other):
        self._validate(other)
        out = []

        for (i1, i2) in zip(self.inner, other.inner):
            out.append(min(i1, i2))
        return Version(out)

    # TODO the proof for this is in the sharing arrangements paper.
    def advance_by(self, frontier):
        if frontier.inner == []:
            return self
        result = frontier.inner[0]
        for elem in frontier.inner:
            result = result.meet(self.join(elem))
        return result

# This keeps the min antichain.
# I fully stole this from frank. TODO: Understand this better
class Antichain:
    def __init__(self, elements):
        self.inner = []
        for element in elements:
            self._insert(element)
    
    def __repr__(self):
        return f'Antichain({self.inner})'
    
    def _insert(self, element):
        for e in self.inner:
            if e.less_equal(element):
                return
        self.inner = [x for x in self.inner if element.less_equal(x) is not True]
        self.inner.append(element)

    # TODO: is it true that the set of versions <= meet(x, y) is the intersection of the set of versions <= x and the set of versions <= y?
    def meet(self, other):
        out = Antichain([])
        for element in self.inner:
            out._insert(element)
        for element in other.inner:
            out._insert(element)

        return out

    # Returns true if other dominates self
    # in other words self < other means
    # self <= other AND self != other
    def less_than(self, other):
        if self.less_equal(other) is not True:
            return False

        for o in other.inner:
            for s in self.inner:
                if s.less_than(o):
                    return True
        return False
    
    def less_equal(self, other):
        for o in other.inner:
            less_equal = False
            for s in self.inner:
                if s.less_equal(o):
                    less_equal = True
            if less_equal == False:
                return False
        return True
    
    def less_equal_version(self, version):
        for elem in self.inner:
            if elem.less_equal(version):
                return True
        return False

class Operator:
    def __init__(self, inputs, output, f, initial_frontier):
        self.inputs = inputs
        self.output = output
        self.f = f
        self.pending_work = False
        self.input_frontiers = [initial_frontier for _ in self.inputs]
        self.output_frontier = initial_frontier

    def run(self):
        self.f()

    def pending_work(self):
        if self.pending_work is True:
            return True
        for input_listener in self.inputs:
            if input_listener.is_empty() is False:
                return True
        return False

    def frontiers(self):
        return (self.input_frontiers, self.output_frontier)

class UnaryOperator(Operator):
    def __init__(self, input_a, output, f, initial_frontier):
        super().__init__([input_a], output, f, initial_frontier)

    def input_messages(self):
        return self.inputs[0].drain()

    def input_frontier(self):
        return self.input_frontiers[0]

    def set_input_frontier(self, frontier):
        self.input_frontiers[0] = frontier

class BinaryOperator(Operator):
    def __init__(self, input_a, input_b, output, f, initial_frontier):
        super().__init__([input_a, input_b], output, f, initial_frontier)

    def input_a_messages(self):
        return self.inputs[0].drain()

    def input_a_frontier(self):
        return self.input_frontiers[0]

    def set_input_a_frontier(self, frontier):
        self.input_frontiers[0] = frontier

    def input_b_messages(self):
        return self.inputs[1].drain()

    def input_b_frontier(self):
        return self.input_frontiers[1]

    def set_input_b_frontier(self, frontier):
        self.input_frontiers[1] = frontier

class LinearUnaryOperator(UnaryOperator):
    def __init__(self, input_a, output, f, initial_frontier):
        def inner():
            for (typ, version, collection) in self.input_messages():
                if typ == MessageType.DATA:
                    result = f(collection)
                    self.output.send_data(version, result)
                elif typ == MessageType.FRONTIER:
                    assert(self.input_frontier().less_equal(version))
                    self.set_input_frontier(version)

            assert(self.output_frontier.less_equal(self.input_frontier()))
            if self.output_frontier.less_than(self.input_frontier()):
                self.output_frontier = self.input_frontier()
                self.output.send_frontier(self.output_frontier)

        super().__init__(input_a, output, inner, initial_frontier)

class MapOperator(LinearUnaryOperator):
    def __init__(self, input_a, output, f, initial_frontier):
        def map_inner(collection):
            return collection.map(f)

        super().__init__(input_a, output, map_inner, initial_frontier)

class FilterOperator(LinearUnaryOperator):
    def __init__(self, input_a, output, f, initial_frontier):
        def filter_inner(collection):
            return collection.filter(f)

        super().__init__(input_a, output, filter_inner, initial_frontier)

class NegateOperator(LinearUnaryOperator):
    def __init__(self, input_a, output, initial_frontier):
        def negate_inner(collection):
            return collection.negate()

        super().__init__(input_a, output, negate_inner, initial_frontier)

class ConcatOperator(BinaryOperator):
    def __init__(self, input_a, input_b, output, initial_frontier):
        def inner():
            for (typ, version, collection) in self.input_a_messages():
                if typ == MessageType.DATA:
                    self.output.send_data(version, collection)
                elif typ == MessageType.FRONTIER:
                    assert(self.input_a_frontier().less_equal(version))
                    self.set_input_a_frontier(version)
            for (typ, version, collection) in self.input_b_messages():
                if typ == MessageType.DATA:
                    self.output.send_data(version, collection)
                elif typ == MessageType.FRONTIER:
                    assert(self.input_b_frontier().less_equal(version))
                    self.set_input_b_frontier(version)

            input_frontier = self.input_a_frontier().meet(self.input_b_frontier())
            assert(self.output_frontier.less_equal(input_frontier))
            if self.output_frontier.less_than(input_frontier):
                self.output_frontier = input_frontier
                self.output.send_frontier(self.output_frontier)

        super().__init__(input_a, input_b, output, inner, initial_frontier)

class JoinOperator(BinaryOperator):
    def __init__(self, input_a, input_b, output, initial_frontier):
        self.index_a = Index()
        self.index_b = Index()

        def inner():
            delta_a = Index()
            delta_b = Index()
            for (typ, version, collection) in self.input_a_messages():
                if typ == MessageType.DATA:
                    for ((key, value), multiplicity) in collection.inner:
                        delta_a.add_value(key, version, (value, multiplicity)) 
                elif typ == MessageType.FRONTIER:
                    assert(self.input_a_frontier().less_equal(version))
                    self.set_input_a_frontier(version)
            for (typ, version, collection) in self.input_b_messages():
                if typ == MessageType.DATA:
                    for ((key, value), multiplicity) in collection.inner:
                         delta_b.add_value(key, version, (value, multiplicity))
                elif typ == MessageType.FRONTIER:
                    assert(self.input_b_frontier().less_equal(version))
                    self.set_input_b_frontier(version)

            results = defaultdict(Collection)
            for (version, collection) in delta_a.join(self.index_b):
                results[version]._extend(collection)

            self.index_a.append(delta_a)

            for (version, collection) in self.index_a.join(delta_b):
                results[version]._extend(collection)
            
            for (version, collection) in results.items():
                self.output.send_data(version, collection)
            self.index_b.append(delta_b)

            input_frontier = self.input_a_frontier().meet(self.input_b_frontier())
            assert(self.output_frontier.less_equal(input_frontier))
            if self.output_frontier.less_than(input_frontier):
                self.output_frontier = input_frontier
                self.output.send_frontier(self.output_frontier)
                # TODO not sure about this compaction logic
                self.index_a.compact(self.output_frontier)
                self.index_b.compact(self.output_frontier)

        super().__init__(input_a, input_b, output, inner, initial_frontier)

if __name__ == '__main__':
    graph = Graph(Antichain([Version([0, 0])]))
    input_a = graph.new_stream()
    output = input_a.map(lambda data: data + 5).filter(lambda data: data % 2 == 0)
    final_output = input_a.negate().concat(output)
    final_output_listener = final_output.connect_to()

    for i in range(0, 10):
        input_a.send_data(Version([0, i]), Collection([(i, 1)]))
        input_a.send_frontier(Antichain([Version([i, 0]), Version([0, i])]))
        graph.step()
        print(final_output_listener.drain())
    
    print('done')
    graph = Graph(Antichain([Version([0, 0])]))
    input_a = graph.new_stream()
    input_b = graph.new_stream()

    output = input_a.join(input_b)
    output_listener = output.connect_to()

    for i in range(0, 10):
        input_a.send_data(Version([0, i]), Collection([((1, i), 2)]))
        input_a.send_data(Version([0, i]), Collection([((2, i), 2)]))

        a_frontier = Antichain([Version([i + 2, 0]), Version([0, i])])
        input_a.send_frontier(a_frontier)
        input_b.send_data(Version([i, 0]), Collection([((1, i + 2), 2)]))
        input_b.send_data(Version([i, 0]), Collection([((2, i + 3), 2)]))
        input_b.send_frontier(Antichain([Version([i, 0]), Version([0, i * 2])]))
        graph.step()
        print(output_listener.drain())
