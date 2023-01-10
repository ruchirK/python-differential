from collections import defaultdict

from collection import Collection
from graph import (
    BinaryOperator,
    CollectionStreamReader,
    CollectionStreamWriter,
    Graph,
    UnaryOperator,
)
from index import Index


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
            self.connect_reader(),
            output.writer(),
            f,
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def filter(self, f):
        output = CollectionStreamBuilder(self.graph)
        operator = FilterOperator(
            self.connect_reader(),
            output.writer(),
            f,
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def negate(self):
        output = CollectionStreamBuilder(self.graph)
        operator = NegateOperator(
            self.connect_reader(),
            output.writer(),
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
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def debug(self, name=""):
        output = CollectionStreamBuilder(self.graph)
        operator = DebugOperator(
            self.connect_reader(),
            output.writer(),
            name,
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
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output

    def count(self):
        output = CollectionStreamBuilder(self.graph)
        operator = CountOperator(
            self.connect_reader(),
            output.writer(),
        )
        self.graph.add_operator(operator)
        self.graph.add_stream(output.connect_reader())
        return output


class GraphBuilder:
    def __init__(self):
        self.streams = []
        self.operators = []

    def new_input(self):
        stream_builder = CollectionStreamBuilder(self)
        self.streams.append(stream_builder.connect_reader())
        return stream_builder, stream_builder.writer()

    def add_operator(self, operator):
        self.operators.append(operator)

    def add_stream(self, stream):
        self.streams.append(stream)

    def finalize(self):
        return Graph(self.streams, self.operators)


class LinearUnaryOperator(UnaryOperator):
    def __init__(self, input_a, output, f):
        def inner():
            for collection in self.input_messages():
                self.output.send_data(f(collection))

        super().__init__(input_a, output, inner)


class MapOperator(LinearUnaryOperator):
    def __init__(self, input_a, output, f):
        def map_inner(collection):
            return collection.map(f)

        super().__init__(input_a, output, map_inner)


class FilterOperator(LinearUnaryOperator):
    def __init__(self, input_a, output, f):
        def filter_inner(collection):
            return collection.filter(f)

        super().__init__(input_a, output, filter_inner)


class NegateOperator(LinearUnaryOperator):
    def __init__(self, input_a, output):
        def negate_inner(collection):
            return collection.negate()

        super().__init__(input_a, output, negate_inner)


class ConcatOperator(BinaryOperator):
    def __init__(self, input_a, input_b, output):
        self.input_a_pending = []
        self.input_b_pending = []

        def inner():
            # This is not internally consistent!
            for collection in self.input_a_messages():
                self.input_a_pending.append(collection)
            for collection in self.input_b_messages():
                self.input_b_pending.append(collection)

            sent = 0
            for (collection_a, collection_b) in zip(
                self.input_a_pending, self.input_b_pending
            ):
                self.output.send_data(collection_a.concat(collection_b))
                sent += 1
            if sent > 0:
                self.input_a_pending = self.input_a_pending[sent:]
                self.input_b_pending = self.input_b_pending[sent:]

        super().__init__(input_a, input_b, output, inner)


class DebugOperator(UnaryOperator):
    def __init__(self, input_a, output, name):
        def inner():
            for collection in self.input_messages():
                print(f"debug {name} data: collection: {collection}")
                self.output.send_data(collection)

        super().__init__(input_a, output, inner)


class JoinOperator(BinaryOperator):
    def __init__(self, input_a, input_b, output):
        self.index_a = Index()
        self.index_b = Index()
        self.input_a_pending = []
        self.input_b_pending = []

        def inner():
            for collection in self.input_a_messages():
                delta_a = Index()
                for ((key, value), multiplicity) in collection._inner:
                    delta_a.add_value(key, (value, multiplicity))
                self.input_a_pending.append(delta_a)
            for collection in self.input_b_messages():
                delta_b = Index()
                for ((key, value), multiplicity) in collection._inner:
                    delta_b.add_value(key, (value, multiplicity))
                self.input_b_pending.append(delta_b)

            sent = 0
            for (delta_a, delta_b) in zip(self.input_a_pending, self.input_b_pending):
                result = Collection()
                result._extend(delta_a.join(self.index_b))
                self.index_a.append(delta_a)
                result._extend(self.index_a.join(delta_b))
                self.index_b.append(delta_b)
                self.output.send_data(result.consolidate())
                sent += 1
                self.index_a.compact()
                self.index_b.compact()

            if sent > 0:
                self.input_a_pending = self.input_a_pending[sent:]
                self.input_b_pending = self.input_b_pending[sent:]

        super().__init__(input_a, input_b, output, inner)


class ReduceOperator(UnaryOperator):
    def __init__(self, input_a, output, f):
        self.index = Index()
        self.index_out = Index()

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
            for collection in self.input_messages():
                keys_todo = set()
                result = []
                for ((key, value), multiplicity) in collection._inner:
                    self.index.add_value(key, (value, multiplicity))
                    keys_todo.add(key)
                keys = [key for key in keys_todo]
                for key in keys:
                    curr = self.index.get(key)
                    curr_out = self.index_out.get(key)
                    out = f(curr)
                    delta = subtract_values(out, curr_out)
                    for (value, multiplicity) in delta:
                        result.append(((key, value), multiplicity))
                        self.index_out.add_value(key, (value, multiplicity))
                self.output.send_data(Collection(result))
                self.index.compact(keys)
                self.index_out.compact(keys)

        super().__init__(input_a, output, inner)


class CountOperator(ReduceOperator):
    def __init__(self, input_a, output):
        def count_inner(vals):
            out = 0
            for (_, diff) in vals:
                out += diff
            return [(out, 1)]

        super().__init__(input_a, output, count_inner)


if __name__ == "__main__":
    graph_builder = GraphBuilder()
    input_a, input_a_writer = graph_builder.new_input()
    output = input_a.map(lambda data: data + 5).filter(lambda data: data % 2 == 0)
    input_a.negate().concat(output).debug("output")
    graph = graph_builder.finalize()

    for i in range(0, 10):
        input_a_writer.send_data(Collection([(i, 1)]))
        graph.step()
    graph_builder = GraphBuilder()
    input_a, input_a_writer = graph_builder.new_input()
    input_b, input_b_writer = graph_builder.new_input()

    output = input_a.join(input_b).count().debug("count")
    graph = graph_builder.finalize()

    for i in range(0, 10):
        input_a_writer.send_data(Collection([((1, i), 2)]))
        input_a_writer.send_data(Collection([((2, i), 2)]))
        input_b_writer.send_data(Collection([((1, i + 2), 2)]))
        input_b_writer.send_data(Collection([((2, i + 3), 2)]))
        graph.step()
    graph.step()
