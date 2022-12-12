from collections import defaultdict, deque

from collection import Collection
from differential_dataflow_1d import MessageType, CollectionStreamListener

class Index:
    def __init__(self):
        self.inner = defaultdict(lambda: defaultdict(list))
        # TODO: take an initial time?
        self.compaction_frontier = None

    def _validate(self, requested_version):
       if self.compaction_frontier is None:
           return True
       if isinstance(requested_version, Antichain):
           assert(self.compaction_frontier.less_equal(requested_version))
       elif isinstance(requested_version, Version):
           assert(self.compaction_frontier.less_equal_version(requested_version))

    def reconstruct_at(self, key, requested_version):
        self._validate(requested_version)
        out = []
        for (version, values) in self.inner[key].items():
            if version.less_equal(requested_version):
                out.extend(values)
        return out

    def values(self, key, version):
        return self.inner[key][version]

    def versions(self, key):
        return [version for version in self.inner[key].keys()]

    def add_value(self, key, version, value):
        self._validate(version)
        self.inner[key][version].append(value)

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
        assert(self.compaction_frontier is None or self.compaction_frontier.less_equal(compaction_frontier))
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
    
    def negate(self):
        output = self.graph.new_stream()
        negate_operator = NegateOperator(self.connect_to(), output, self.graph.frontier())
        self.graph.add_operator(negate_operator)
        return output

    def debug(self, name=''):
        output = self.graph.new_stream()
        debug_operator = DebugOperator(self.connect_to(), output, name, self.graph.frontier())
        self.graph.add_operator(debug_operator)
        return output
    
    def consolidate(self):
        output = self.graph.new_stream()
        consolidate_operator = ConsolidateOperator(self.connect_to(), output, self.graph.frontier())
        self.graph.add_operator(consolidate_operator)
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
    
    def count(self):
        output = self.graph.new_stream()
        count_operator = CountOperator(self.connect_to(), output, self.graph.frontier())
        self.graph.add_operator(count_operator)
        return output
    def distinct(self):
        output = self.graph.new_stream()
        distinct_operator = DistinctOperator(self.connect_to(), output, self.graph.frontier())
        self.graph.add_operator(distinct_operator)
        return output

    # creates a new scope and brings a collection into it.
    # to be used only for iterate.
    def _enter(self):
        new_frontier = self.graph.frontier().extend()
        self.graph.push_frontier(new_frontier)
        output = self.graph.new_stream()
        ingress_operator = IngressOperator(self.connect_to(), output, self.graph.frontier())
        self.graph.add_operator(ingress_operator)
        return output
    
    def _leave(self):
        self.graph.pop_frontier()
        output = self.graph.new_stream()
        egress_operator = EgressOperator(self.connect_to(), output, self.graph.frontier())
        self.graph.add_operator(egress_operator)
        return output

    def iterate(self, f):
        feedback_stream = self.graph.new_stream()
        entered = self._enter().concat(feedback_stream)
        result = f(entered)
        feedback_operator = FeedbackOperator(result.connect_to(), 1, feedback_stream, graph.frontier())
        self.graph.add_operator(feedback_operator)
        return result._leave()

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

    # TODO need to make sure it respects partial order
    def __lt__(self, other):
        return self.inner.__lt__(other.inner)
    
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
        if frontier.inner == ():
            return self
        result = frontier.inner[0]
        for elem in frontier.inner:
            result = result.meet(self.join(elem))
        return result

    def extend(self):
        elements = [e for e in self.inner]
        elements.append(0)
        return Version(elements)
    
    def truncate(self):
        elements = [e for e in self.inner]
        elements.pop()
        return Version(elements)

    def apply_step(self, step):
        assert(step > 0)
        elements = [e for e in self.inner]
        elements[-1] += step
        return Version(elements)

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

    def _equals(self, other):
        elements_1 = [x for x in self.inner]
        elements_2 = [y for y in other.inner]

        if len(elements_1) != len(elements_2):
            return False
        elements_1.sort()
        elements_2.sort()

        for (x, y) in zip(elements_1, elements_2):
            if x != y:
                return False
        return True
    # Returns true if other dominates self
    # in other words self < other means
    # self <= other AND self != other
    def less_than(self, other):
        if self.less_equal(other) is not True:
            return False

        if self._equals(other):
            return False

        return True
    
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

    def extend(self):
        out = Antichain([])
        for elem in self.inner:
            out._insert(elem.extend())
        return out
        
    def truncate(self):
        out = Antichain([])
        for elem in self.inner:
            out._insert(elem.truncate())
        return out

    def apply_step(self, step):
        out = Antichain([])
        for elem in self.inner:
            out._insert(elem.apply_step(step))
        return out

    def _elements(self):
        return [x for x in self.inner]

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

class ConsolidateOperator(UnaryOperator):
    def __init__(self, input_a, output, initial_frontier):
        self.collections = defaultdict(Collection)

        def inner():
            for (typ, version, collection) in self.input_messages():
                if typ == MessageType.DATA:
                    self.collections[version]._extend(collection)
                elif typ == MessageType.FRONTIER:
                    assert(self.input_frontier().less_equal(version))
                    self.set_input_frontier(version)
            finished_versions = [version for version in self.collections.keys() if self.input_frontier().less_equal_version(version) is not True]
            for version in finished_versions:
                collection = self.collections.pop(version).consolidate()
                self.output.send_data(version, collection)
            assert(self.output_frontier.less_equal(self.input_frontier()))
            if self.output_frontier.less_than(self.input_frontier()):
                self.output_frontier = self.input_frontier()
                self.output.send_frontier(self.output_frontier)
        super().__init__(input_a, output, inner, initial_frontier)

class DebugOperator(UnaryOperator):
    def __init__(self, input_a, output, name, initial_frontier):
        def inner():
            for (typ, version, collection) in self.input_messages():
                if typ == MessageType.DATA:
                    print(f'debug {name} data: version: {version} collection: {collection}')
                    self.output.send_data(version, collection)
                elif typ == MessageType.FRONTIER:
                    assert(self.input_frontier().less_equal(version))
                    self.set_input_frontier(version)
                    print(f'debug {name} notification: frontier {version}')
                    assert(self.output_frontier.less_equal(self.input_frontier()))
                    if self.output_frontier.less_than(self.input_frontier()):
                        self.output_frontier = self.input_frontier()
                        self.output.send_frontier(self.output_frontier)
        super().__init__(input_a, output, inner, initial_frontier)

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

class ReduceOperator(UnaryOperator):
    def __init__(self, input_a, output, f, initial_frontier):
        self.index = Index()
        self.index_out = Index()
        self.keys_todo = defaultdict(set)
        def subtract_values(first, second):
            result = defaultdict(int)
            for (v1, m1) in first:
                result[v1] += m1
            for (v2, m2) in second:
                result[v2] -= m2

            return [(val, multiplicity) for (val, multiplicity) in result.items() if multiplicity != 0]

        def inner():
            for (typ, version, collection) in self.input_messages():
                if typ == MessageType.DATA:
                    for ((key, value), multiplicity) in collection.inner:
                        self.index.add_value(key, version, (value, multiplicity))
                        self.keys_todo[version].add(key)
                        for v2 in self.index.versions(key):
                            new_version = version.join(v2)
                            self.keys_todo[version.join(v2)].add(key)
                elif typ == MessageType.FRONTIER:
                    assert(self.input_frontier().less_equal(version))
                    self.set_input_frontier(version)

            finished_versions = [version for version in self.keys_todo.keys() if self.input_frontier().less_equal_version(version) is not True]

            finished_versions.sort()
            for version in finished_versions:
               keys = self.keys_todo.pop(version)
               result = []
               for key in keys:
                   curr = self.index.reconstruct_at(key, version)
                   curr_out = self.index_out.reconstruct_at(key, version)
                   out = f(curr)
                   delta = subtract_values(out, curr_out)
                   for (value, multiplicity) in delta:
                       result.append(((key, value), multiplicity))
                       self.index_out.add_value(key, version, (value, multiplicity))
               if result != []:
                   self.output.send_data(version, Collection(result))
            
            assert(self.output_frontier.less_equal(self.input_frontier()))
            if self.output_frontier.less_than(self.input_frontier()):
                self.output_frontier = self.input_frontier()
                self.output.send_frontier(self.output_frontier)
                self.index.compact(self.output_frontier)
                self.index_out.compact(self.output_frontier)

        super().__init__(input_a, output, inner, initial_frontier)

class CountOperator(ReduceOperator):
    def __init__(self, input_a, output, initial_frontier):
        def count_inner(vals):
            out = 0
            for (_, diff) in vals:
                out += diff
            return [(out, 1)]
        super().__init__(input_a, output, count_inner, initial_frontier)

class DistinctOperator(ReduceOperator):
    def __init__(self, input_a, output, initial_frontier):
        def distinct_inner(vals):
            consolidated = defaultdict(int)
            for (val, diff) in vals:
                consolidated[val] += diff
            for (val, diff) in consolidated.items():
                assert(diff >= 0)
            return [(val, 1) for (val, diff) in consolidated.items() if diff > 0]
        super().__init__(input_a, output, distinct_inner, initial_frontier)

class FeedbackOperator(UnaryOperator):
    def __init__(self, input_a, step, output, initial_frontier):
        self.versions_with_data = set()
        def inner():
            for (typ, version, collection) in self.input_messages():
                if typ == MessageType.DATA:
                    self.output.send_data(version.apply_step(step), collection)
                    self.versions_with_data.add(version.apply_step(step))
                elif typ == MessageType.FRONTIER:
                    assert(self.input_frontier().less_equal(version))
                    self.set_input_frontier(version)

            candidate_output_frontier = self.input_frontier().apply_step(step)
            # TODO XXX not sure about this!
            #print(f'versions with data: {self.versions_with_data}')
            elements = candidate_output_frontier._elements()
            elements.sort()
            candidate = set()
            candidate.add(elements[-1])
            for elem in elements:
                to_remove = [x for x in self.versions_with_data if x.less_than(elem)]
                #print(f'elem: {elem} to_remove: {to_remove}')
                if len(to_remove) > 0:
                    candidate.add(elem)
                    for x in to_remove:
                        self.versions_with_data.remove(x)
            candidate_output_frontier = Antichain([x for x in candidate])
            #print(f'candidate_output_frontier: {candidate_output_frontier} output_frontiier: {self.output_frontier}')
            
            assert(self.output_frontier.less_equal(candidate_output_frontier))
            if self.output_frontier.less_than(candidate_output_frontier):
                self.output_frontier = candidate_output_frontier
                self.output.send_frontier(self.output_frontier)

        super().__init__(input_a, output, inner, initial_frontier)

    def connect_loop(output):
        self.output = output 

class IngressOperator(UnaryOperator):
    def __init__(self, input_a, output, initial_frontier):
        def inner():        
            for (typ, version, collection) in self.input_messages():
                if typ == MessageType.DATA:
                    new_version = version.extend()
                    self.output.send_data(new_version, collection)
                    self.output.send_data(new_version.apply_step(1), collection.negate())
                elif typ == MessageType.FRONTIER:
                    new_frontier = version.extend()
                    assert(self.input_frontier().less_equal(new_frontier))
                    self.set_input_frontier(new_frontier)

            assert(self.output_frontier.less_equal(self.input_frontier()))
            if self.output_frontier.less_than(self.input_frontier()):
                self.output_frontier = self.input_frontier()
                self.output.send_frontier(self.output_frontier)
        super().__init__(input_a, output, inner, initial_frontier)

class EgressOperator(UnaryOperator):
    def __init__(self, input_a, output, initial_frontier):
        def inner():        
            for (typ, version, collection) in self.input_messages():
                if typ == MessageType.DATA:
                    new_version = version.truncate()
                    self.output.send_data(new_version, collection)
                elif typ == MessageType.FRONTIER:
                    new_frontier = version.truncate()
                    assert(self.input_frontier().less_equal(new_frontier))
                    self.set_input_frontier(new_frontier)

            assert(self.output_frontier.less_equal(self.input_frontier()))
            if self.output_frontier.less_than(self.input_frontier()):
                self.output_frontier = self.input_frontier()
                self.output.send_frontier(self.output_frontier)
        super().__init__(input_a, output, inner, initial_frontier)
        
if __name__ == '__main__':

    v0_0 = Version([0, 0])
    v1_0 = Version([1, 0])
    v0_1 = Version([0, 1])
    v1_1 = Version([1, 1])
    v2_0 = Version([2, 0])

    assert(v0_0.less_than(v1_0))
    assert(v0_0.less_than(v0_1))
    assert(v0_0.less_than(v1_1))
    assert(v0_0.less_equal(v1_0))
    assert(v0_0.less_equal(v0_1))
    assert(v0_0.less_equal(v1_1))

    assert(v1_0.less_than(v1_0) is not True)
    assert(v1_0.less_equal(v1_0))
    assert(v1_0.less_equal(v0_1) is not True)
    assert(v0_1.less_equal(v1_0) is not True)
    assert(v0_1.less_equal(v1_1))
    assert(v1_0.less_equal(v1_1))
    assert(v0_0.less_equal(v1_1))

    assert(Antichain([v0_0]).less_equal(Antichain([v1_0])))
    assert(Antichain([v0_0])._equals(Antichain([v1_0])) is not True)
    assert(Antichain([v0_0]).less_than(Antichain([v1_0])))
    assert(Antichain([v2_0, v1_1]).less_than(Antichain([v2_0])))

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

    output = input_a.join(input_b).count()
    output_listener = output.connect_to()

    for i in range(0, 2):
        input_a.send_data(Version([0, i]), Collection([((1, i), 2)]))
        input_a.send_data(Version([0, i]), Collection([((2, i), 2)]))

        a_frontier = Antichain([Version([i + 2, 0]), Version([0, i])])
        input_a.send_frontier(a_frontier)
        input_b.send_data(Version([i, 0]), Collection([((1, i + 2), 2)]))
        input_b.send_data(Version([i, 0]), Collection([((2, i + 3), 2)]))
        input_b.send_frontier(Antichain([Version([i, 0]), Version([0, i * 2])]))
        graph.step()
        print(output_listener.drain())
        
    input_a.send_frontier(Antichain([Version([11, 11])]))
    input_b.send_frontier(Antichain([Version([11, 11])]))
    graph.step()
    print(output_listener.drain())
    graph = Graph(Antichain([Version(0)]))
    input_a = graph.new_stream()
    def geometric_series(collection):
        return collection.map(lambda data: data + data) \
            .concat(collection)                     \
            .filter(lambda data: data <= 100)          \
            .map(lambda data: (data, ()))             \
            .distinct()                               \
            .map(lambda data: data[0])                \
            .consolidate()

    output = input_a.iterate(geometric_series).debug('output')
    output_listener = output.connect_to()
    input_a.send_data(Version(0), Collection([(1, 1)]))
    input_a.send_frontier(Antichain([Version(1)]))

    for i in range(0, 10):
        graph.step()
    print('done done')
