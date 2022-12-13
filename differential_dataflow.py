from collections import defaultdict, deque

from base_operator import UnaryOperator, BinaryOperator
from collection import Collection
from differential_dataflow_1d import MessageType, CollectionStreamListener
from index import Index
from order import Version, Antichain


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
        for q in self.queues:
            q.appendleft((MessageType.DATA, version, collection))

    def send_frontier(self, frontier):
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
        filter_operator = FilterOperator(
            self.connect_to(), output, f, self.graph.frontier()
        )
        self.graph.add_operator(filter_operator)
        return output

    def negate(self):
        output = self.graph.new_stream()
        negate_operator = NegateOperator(
            self.connect_to(), output, self.graph.frontier()
        )
        self.graph.add_operator(negate_operator)
        return output

    def debug(self, name=""):
        output = self.graph.new_stream()
        debug_operator = DebugOperator(
            self.connect_to(), output, name, self.graph.frontier()
        )
        self.graph.add_operator(debug_operator)
        return output

    def consolidate(self):
        output = self.graph.new_stream()
        consolidate_operator = ConsolidateOperator(
            self.connect_to(), output, self.graph.frontier()
        )
        self.graph.add_operator(consolidate_operator)
        return output

    def concat(self, other):
        # TODO check that these are edges on the same graph
        output = self.graph.new_stream()
        concat_operator = ConcatOperator(
            self.connect_to(), other.connect_to(), output, self.graph.frontier()
        )
        self.graph.add_operator(concat_operator)
        return output

    def join(self, other):
        # TODO check that these are edges on the same graph
        output = self.graph.new_stream()
        join_operator = JoinOperator(
            self.connect_to(), other.connect_to(), output, self.graph.frontier()
        )
        self.graph.add_operator(join_operator)
        return output

    def count(self):
        output = self.graph.new_stream()
        count_operator = CountOperator(self.connect_to(), output, self.graph.frontier())
        self.graph.add_operator(count_operator)
        return output

    def distinct(self):
        output = self.graph.new_stream()
        distinct_operator = DistinctOperator(
            self.connect_to(), output, self.graph.frontier()
        )
        self.graph.add_operator(distinct_operator)
        return output

    # creates a new scope and brings a collection into it.
    # to be used only for iterate.
    def _enter(self):
        new_frontier = self.graph.frontier().extend()
        self.graph.push_frontier(new_frontier)
        output = self.graph.new_stream()
        ingress_operator = IngressOperator(
            self.connect_to(), output, self.graph.frontier()
        )
        self.graph.add_operator(ingress_operator)
        return output

    def _leave(self):
        self.graph.pop_frontier()
        output = self.graph.new_stream()
        egress_operator = EgressOperator(
            self.connect_to(), output, self.graph.frontier()
        )
        self.graph.add_operator(egress_operator)
        return output

    def iterate(self, f):
        feedback_stream = self.graph.new_stream()
        entered = self._enter().concat(feedback_stream)
        result = f(entered)
        feedback_operator = FeedbackOperator(
            result.connect_to(), 1, feedback_stream, graph.frontier()
        )
        self.graph.add_operator(feedback_operator)
        return result._leave()


class LinearUnaryOperator(UnaryOperator):
    def __init__(self, input_a, output, f, initial_frontier):
        def inner():
            for (typ, version, collection) in self.input_messages():
                if typ == MessageType.DATA:
                    result = f(collection)
                    self.output.send_data(version, result)
                elif typ == MessageType.FRONTIER:
                    assert self.input_frontier().less_equal(version)
                    self.set_input_frontier(version)

            assert self.output_frontier.less_equal(self.input_frontier())
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
                    assert self.input_a_frontier().less_equal(version)
                    self.set_input_a_frontier(version)
            for (typ, version, collection) in self.input_b_messages():
                if typ == MessageType.DATA:
                    self.output.send_data(version, collection)
                elif typ == MessageType.FRONTIER:
                    assert self.input_b_frontier().less_equal(version)
                    self.set_input_b_frontier(version)

            input_frontier = self.input_a_frontier().meet(self.input_b_frontier())
            assert self.output_frontier.less_equal(input_frontier)
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
                    assert self.input_frontier().less_equal(version)
                    self.set_input_frontier(version)
            finished_versions = [
                version
                for version in self.collections.keys()
                if self.input_frontier().less_equal_version(version) is not True
            ]
            for version in finished_versions:
                collection = self.collections.pop(version).consolidate()
                self.output.send_data(version, collection)
            assert self.output_frontier.less_equal(self.input_frontier())
            if self.output_frontier.less_than(self.input_frontier()):
                self.output_frontier = self.input_frontier()
                self.output.send_frontier(self.output_frontier)

        super().__init__(input_a, output, inner, initial_frontier)


class DebugOperator(UnaryOperator):
    def __init__(self, input_a, output, name, initial_frontier):
        def inner():
            for (typ, version, collection) in self.input_messages():
                if typ == MessageType.DATA:
                    print(
                        f"debug {name} data: version: {version} collection: {collection}"
                    )
                    self.output.send_data(version, collection)
                elif typ == MessageType.FRONTIER:
                    assert self.input_frontier().less_equal(version)
                    self.set_input_frontier(version)
                    print(f"debug {name} notification: frontier {version}")
                    assert self.output_frontier.less_equal(self.input_frontier())
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
                    for ((key, value), multiplicity) in collection._inner:
                        delta_a.add_value(key, version, (value, multiplicity))
                elif typ == MessageType.FRONTIER:
                    assert self.input_a_frontier().less_equal(version)
                    self.set_input_a_frontier(version)
            for (typ, version, collection) in self.input_b_messages():
                if typ == MessageType.DATA:
                    for ((key, value), multiplicity) in collection._inner:
                        delta_b.add_value(key, version, (value, multiplicity))
                elif typ == MessageType.FRONTIER:
                    assert self.input_b_frontier().less_equal(version)
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
            assert self.output_frontier.less_equal(input_frontier)
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

            return [
                (val, multiplicity)
                for (val, multiplicity) in result.items()
                if multiplicity != 0
            ]

        def inner():
            for (typ, version, collection) in self.input_messages():
                if typ == MessageType.DATA:
                    for ((key, value), multiplicity) in collection._inner:
                        self.index.add_value(key, version, (value, multiplicity))
                        self.keys_todo[version].add(key)
                        for v2 in self.index.versions(key):
                            new_version = version.join(v2)
                            self.keys_todo[version.join(v2)].add(key)
                elif typ == MessageType.FRONTIER:
                    assert self.input_frontier().less_equal(version)
                    self.set_input_frontier(version)

            finished_versions = [
                version
                for version in self.keys_todo.keys()
                if self.input_frontier().less_equal_version(version) is not True
            ]

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

            assert self.output_frontier.less_equal(self.input_frontier())
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
                assert diff >= 0
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
                    assert self.input_frontier().less_equal(version)
                    self.set_input_frontier(version)

            candidate_output_frontier = self.input_frontier().apply_step(step)
            # TODO XXX not sure about this!
            # print(f'versions with data: {self.versions_with_data}')
            elements = candidate_output_frontier._elements()
            elements.sort()
            candidate = set()
            candidate.add(elements[-1])
            for elem in elements:
                to_remove = [x for x in self.versions_with_data if x.less_than(elem)]
                # print(f'elem: {elem} to_remove: {to_remove}')
                if len(to_remove) > 0:
                    candidate.add(elem)
                    for x in to_remove:
                        self.versions_with_data.remove(x)
            candidate_output_frontier = Antichain([x for x in candidate])
            # print(f'candidate_output_frontier: {candidate_output_frontier} output_frontiier: {self.output_frontier}')

            assert self.output_frontier.less_equal(candidate_output_frontier)
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
                    self.output.send_data(
                        new_version.apply_step(1), collection.negate()
                    )
                elif typ == MessageType.FRONTIER:
                    new_frontier = version.extend()
                    assert self.input_frontier().less_equal(new_frontier)
                    self.set_input_frontier(new_frontier)

            assert self.output_frontier.less_equal(self.input_frontier())
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
                    assert self.input_frontier().less_equal(new_frontier)
                    self.set_input_frontier(new_frontier)

            assert self.output_frontier.less_equal(self.input_frontier())
            if self.output_frontier.less_than(self.input_frontier()):
                self.output_frontier = self.input_frontier()
                self.output.send_frontier(self.output_frontier)

        super().__init__(input_a, output, inner, initial_frontier)


if __name__ == "__main__":
    graph = Graph(Antichain([Version([0, 0])]))
    input_a = graph.new_stream()
    output = input_a.map(lambda data: data + 5).filter(lambda data: data % 2 == 0)
    input_a.negate().concat(output).debug("output")

    for i in range(0, 10):
        input_a.send_data(Version([0, i]), Collection([(i, 1)]))
        input_a.send_frontier(Antichain([Version([i, 0]), Version([0, i])]))
        graph.step()

    graph = Graph(Antichain([Version([0, 0])]))
    input_a = graph.new_stream()
    input_b = graph.new_stream()

    input_a.join(input_b).count().debug("count")

    for i in range(0, 2):
        input_a.send_data(Version([0, i]), Collection([((1, i), 2)]))
        input_a.send_data(Version([0, i]), Collection([((2, i), 2)]))

        a_frontier = Antichain([Version([i + 2, 0]), Version([0, i])])
        input_a.send_frontier(a_frontier)
        input_b.send_data(Version([i, 0]), Collection([((1, i + 2), 2)]))
        input_b.send_data(Version([i, 0]), Collection([((2, i + 3), 2)]))
        input_b.send_frontier(Antichain([Version([i, 0]), Version([0, i * 2])]))
        graph.step()

    input_a.send_frontier(Antichain([Version([11, 11])]))
    input_b.send_frontier(Antichain([Version([11, 11])]))
    graph.step()
    graph = Graph(Antichain([Version(0)]))
    input_a = graph.new_stream()

    def geometric_series(collection):
        return (
            collection.map(lambda data: data + data)
            .concat(collection)
            .filter(lambda data: data <= 100)
            .map(lambda data: (data, ()))
            .distinct()
            .map(lambda data: data[0])
            .consolidate()
        )

    output = input_a.iterate(geometric_series).debug("iterate")
    input_a.send_data(Version(0), Collection([(1, 1)]))
    input_a.send_frontier(Antichain([Version(1)]))

    for i in range(0, 10):
        graph.step()
