from collections import deque
from enum import Enum


class MessageType(Enum):
    DATA = 1
    FRONTIER = 2


class CollectionStreamReader:
    def __init__(self, queue):
        self._queue = queue

    def drain(self):
        out = []
        while len(self._queue) > 0:
            out.append(self._queue.pop())

        return out

    def is_empty(self):
        return len(self._queue) == 0


class CollectionStreamWriter:
    def __init__(self):
        self._queues = []

    def send_data(self, version, collection):
        for q in self._queues:
            q.appendleft((MessageType.DATA, version, collection))

    def send_frontier(self, frontier):
        for q in self._queues:
            q.appendleft((MessageType.FRONTIER, frontier, []))

    def _new_reader(self):
        q = deque()
        self._queues.append(q)
        return CollectionStreamReader(q)


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


class Graph:
    def __init__(self, streams, operators):
        self.streams = streams
        self.operators = operators

    def step(self):
        for op in self.operators:
            op.run()
