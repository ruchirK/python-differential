from collections import defaultdict

from collection import Collection
from graph import (
    BinaryOperator,
    CollectionStreamReader,
    CollectionStreamWriter,
    Graph,
    MessageType,
    UnaryOperator,
)
from index import Index1D as Index


class CollectionStreamBuilder:
    def __init__(self, graph):
        self._writer = CollectionStreamWriter()
        self.graph = graph

    def connect_reader(self):
        return self._writer._new_reader()

    def writer(self):
        return self._writer

    def map(self, f):
        output = CollectionStreamBuilder(self.graph)
        operator = MapOperator(
            self.connect_reader(), output.writer(), f, self.graph.frontier()
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def filter(self, f):
        output = CollectionStreamBuilder(self.graph)
        operator = FilterOperator(
            self.connect_reader(), output.writer(), f, self.graph.frontier()
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def negate(self):
        output = CollectionStreamBuilder(self.graph)
        operator = NegateOperator(
            self.connect_reader(), output.writer(), self.graph.frontier()
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def concat(self, other):
        assert id(self.graph) == id(other.graph)
        output = CollectionStreamBuilder(self.graph)
        operator = ConcatOperator(
            self.connect_reader(),
            other.connect_reader(),
            output.writer(),
            self.graph.frontier(),
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def debug(self, name=""):
        output = CollectionStreamBuilder(self.graph)
        operator = DebugOperator(
            self.connect_reader(), output.writer(), name, self.graph.frontier()
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def join(self, other):
        assert id(self.graph) == id(other.graph)
        output = CollectionStreamBuilder(self.graph)
        operator = JoinOperator(
            self.connect_reader(),
            other.connect_reader(),
            output.writer(),
            self.graph.frontier(),
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def count(self):
        output = CollectionStreamBuilder(self.graph)
        operator = CountOperator(
            self.connect_reader(), output.writer(), self.graph.frontier()
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output


class GraphBuilder:
    def __init__(self, initial_frontier):
        self.streams = []
        self.operators = []
        self.frontier_stack = [initial_frontier]

    def new_input(self):
        stream_builder = CollectionStreamBuilder(self)
        self.streams.append(stream_builder.connect_reader())
        return stream_builder, stream_builder.writer()

    def add_operator(self, operator):
        self.operators.append(operator)

    def add_stream(self, stream):
        self.streams.append(stream)

    def frontier(self):
        return self.frontier_stack[-1]

    def push_frontier(self, new_frontier):
        self.frontier_stack.append(new_frontier)

    def pop_frontier(self):
        self.frontier_stack.pop()

    def finalize(self):
        return Graph(self.streams, self.operators)


class LinearUnaryOperator(UnaryOperator):
    def __init__(self, input_a, output, f, initial_frontier):
        def inner():
            for (typ, msg) in self.input_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    self.output.send_data(version, f(collection))
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    self.set_input_frontier(frontier)

            if self.input_frontier() > self.output_frontier:
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
            for (typ, msg) in self.input_a_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    self.output.send_data(version, collection)
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    self.set_input_a_frontier(frontier)
            for (typ, msg) in self.input_b_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    self.output.send_data(version, collection)
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    self.set_input_b_frontier(version)

            min_input_frontier = min(self.input_a_frontier(), self.input_b_frontier())
            if min_input_frontier > self.output_frontier:
                self.output_frontier = min_input_frontier
                self.output.send_frontier(self.output_frontier)

        super().__init__(input_a, input_b, output, inner, initial_frontier)


class DebugOperator(UnaryOperator):
    def __init__(self, input_a, output, name, initial_frontier):
        def inner():
            for (typ, msg) in self.input_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    print(
                        f"debug {name} data: version: {version} collection: {collection}"
                    )
                    self.output.send_data(version, collection)
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    assert self.input_frontier() <= frontier
                    self.set_input_frontier(frontier)
                    print(f"debug {name} notification: frontier {version}")
                    assert self.output_frontier <= self.input_frontier()
                    if self.output_frontier < self.input_frontier():
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
            for (typ, msg) in self.input_a_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    for ((key, value), multiplicity) in collection._inner:
                        delta_a.add_value(key, version, (value, multiplicity))
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    self.set_input_a_frontier(msg)
            for (typ, msg) in self.input_b_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    for ((key, value), multiplicity) in collection._inner:
                        delta_b.add_value(key, version, (value, multiplicity))
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    self.set_input_b_frontier(frontier)

            results = defaultdict(Collection)
            for (version, collection) in delta_a.join(self.index_b):
                results[version]._extend(collection)

            self.index_a.append(delta_a)

            for (version, collection) in self.index_a.join(delta_b):
                results[version]._extend(collection)

            for (version, collection) in results.items():
                self.output.send_data(version, collection)
            self.index_b.append(delta_b)

            min_input_frontier = min(self.input_a_frontier(), self.input_b_frontier())
            if min_input_frontier > self.output_frontier:
                self.output_frontier = min_input_frontier
                self.output.send_frontier(self.output_frontier)
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
            for (typ, msg) in self.input_messages():
                if typ == MessageType.DATA:
                    version, collection = msg
                    for ((key, value), multiplicity) in collection._inner:
                        self.index.add_value(key, version, (value, multiplicity))
                        self.keys_todo[version].add(key)
                elif typ == MessageType.FRONTIER:
                    frontier = msg
                    self.set_input_frontier(frontier)

            finished_versions = [
                version
                for version in self.keys_todo.keys()
                if version < self.input_frontier()
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

            if self.input_frontier() > self.output_frontier:
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


if __name__ == "__main__":
    graph_builder = GraphBuilder(0)
    input_a, input_a_writer = graph_builder.new_input()
    output = input_a.map(lambda data: data + 5).filter(lambda data: data % 2 == 0)
    input_a.negate().concat(output).debug("output")
    graph = graph_builder.finalize()

    for i in range(0, 10):
        input_a_writer.send_data(i, Collection([(i, 1)]))
        input_a_writer.send_frontier(i)
        graph.step()
    graph_builder = GraphBuilder(0)
    input_a, input_a_writer = graph_builder.new_input()
    input_b, input_b_writer = graph_builder.new_input()

    output = input_a.join(input_b).count().debug("count")
    graph = graph_builder.finalize()

    for i in range(0, 10):
        input_a_writer.send_data(i, Collection([((1, i), 2)]))
        input_a_writer.send_data(i, Collection([((2, i), 2)]))
        input_b_writer.send_data(i, Collection([((1, i + 2), 2)]))
        input_b_writer.send_data(i, Collection([((2, i + 3), 2)]))
        input_a_writer.send_frontier(i)
        input_b_writer.send_frontier(i)
        graph.step()
    input_a_writer.send_frontier(11)
    input_b_writer.send_frontier(11)
    graph.step()
