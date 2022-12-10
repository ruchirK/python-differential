from collections import defaultdict, deque
from enum import Enum
import random

from collection import Collection
from collection_trace import Index

class MessageType(Enum):
    DATA = 1
    FRONTIER = 2

class CollectionStreamListener:
    def __init__(self, queue):
        self.inner = queue

    def drain(self):
        out = []
        while len(self.inner) > 0:
            out.append(self.inner.pop())

        return out

    def is_empty(self):
        return len(self.queue) == 0

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

    def count(self):
        output = self.graph.new_stream()
        count_operator = CountOperator(self.connect_to(), output, self.graph.frontier())
        self.graph.add_operator(count_operator)
        return output

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
                    self.set_input_frontier(version)

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
            for (typ, version, collection) in self.input_a_messages():
                if typ == MessageType.DATA:
                    self.output.send_data(version, collection)
                elif typ == MessageType.FRONTIER:
                    self.set_input_a_frontier(version)
            for (typ, version, collection) in self.input_b_messages():
                if typ == MessageType.DATA:
                    self.output.send_data(version, collection)
                elif typ == MessageType.FRONTIER:
                    self.set_input_b_frontier(version)

            min_input_frontier = min(self.input_a_frontier(), self.input_b_frontier())
            if min_input_frontier > self.output_frontier:
                self.output_frontier = min_input_frontier
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
                    self.set_input_a_frontier(version)
            for (typ, version, collection) in self.input_b_messages():
                if typ == MessageType.DATA:
                    for ((key, value), multiplicity) in collection.inner:
                         delta_b.add_value(key, version, (value, multiplicity))
                elif typ == MessageType.FRONTIER:
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

            return [(val, multiplicity) for (val, multiplicity) in result.items() if multiplicity != 0]

        def inner():
            for (typ, version, collection) in self.input_messages():
                if typ == MessageType.DATA:
                    for ((key, value), multiplicity) in collection.inner:
                        self.index.add_value(key, version, (value, multiplicity))
                        self.keys_todo[version].add(key)
                elif typ == MessageType.FRONTIER:
                    self.set_input_frontier(version)

            finished_versions = [version for version in self.keys_todo.keys() if version < self.input_frontier()]

            finished_versions.sort()
            for version in finished_versions:
               keys = self.keys_todo.pop(version)
               result = []
               for key in keys:
                   curr = self.index.reconstruct_at(key, version)
                   curr_out = self.index_out.reconstruct_at(key, version)
                   out = f(curr)
                   delta = subtract_values(out, curr_out)
                   #print(f"key: {key} curr: {curr} curr_out: {curr_out} result: {result} delta: {delta}")
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


if __name__ == '__main__':
    graph = Graph(0)
    input_a = graph.new_stream()
    output = input_a.map(lambda data: data + 5).filter(lambda data: data % 2 == 0)
    final_output = input_a.negate().concat(output)
    final_output_listener = final_output.connect_to()

    for i in range(0, 10):
        input_a.send_data(i, Collection([(i, 1)]))
        input_a.send_frontier(i)
        graph.step()
        print(final_output_listener.drain())
    print("done")
    graph = Graph(0)
    input_a = graph.new_stream()
    input_b = graph.new_stream()

    output = input_a.join(input_b)
    output_listener = output.connect_to()

    for i in range(0, 10):
        input_a.send_data(i, Collection([((1, i), 2)]))
        input_a.send_data(i, Collection([((2, i), 2)]))
        input_b.send_data(i, Collection([((1, i +2), 2)]))
        input_b.send_data(i, Collection([((2, i +3), 2)]))
        input_a.send_frontier(i)
        input_b.send_frontier(i)
        graph.step()
        print(output_listener.drain())
