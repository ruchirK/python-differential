"""The implementation of dataflow graph edge, node, and graph objects, used to run a dataflow program."""

from collections import deque


class DifferenceStreamReader:
    """A read handle to a dataflow edge that receives data from a writer.

    The data received over this edge are Collection objects that represent difference
    collections representing a single logical collection undergoing changes.
    """

    def __init__(self, queue):
        self._queue = queue

    def drain(self):
        out = []
        while len(self._queue) > 0:
            out.append(self._queue.pop())

        return out

    def is_empty(self):
        return len(self._queue) == 0


class DifferenceStreamWriter:
    """A write handle to a dataflow edge that is allowed to publish data."""

    def __init__(self):
        self._queues = []

    def send_data(self, collection):
        for q in self._queues:
            q.appendleft(collection)

    def _new_reader(self):
        q = deque()
        self._queues.append(q)
        return DifferenceStreamReader(q)


class Operator:
    """A generic implementation of a dataflow operator (node) that has multiple incoming edges (read handles) and
    one outgoing edge (write handle).
    """

    def __init__(self, inputs, output, f):
        self.inputs = inputs
        self.output = output
        self.f = f
        self.pending_work = False

    def run(self):
        self.f()

    def pending_work(self):
        if self.pending_work is True:
            return True
        for input_listener in self.inputs:
            if input_listener.is_empty() is False:
                return True
        return False


class UnaryOperator(Operator):
    """A convenience implementation of a dataflow operator that has a handle to one
    incoming stream of data, and one handle to an outgoing stream of data.
    """

    def __init__(self, input_a, output, f):
        super().__init__([input_a], output, f)

    def input_messages(self):
        return self.inputs[0].drain()


class BinaryOperator(Operator):
    """A convenience implementation of a dataflow operator that has a handle to two
    incoming streams of data, and one handle to an outgoing stream of data.
    """

    def __init__(self, input_a, input_b, output, f):
        super().__init__([input_a, input_b], output, f)

    def input_a_messages(self):
        return self.inputs[0].drain()

    def input_b_messages(self):
        return self.inputs[1].drain()


class Graph:
    """An implementation of a dataflow graph.

    This implementation needs to keep the entire set of nodes so that they
    may be run, and only keeps a set of read handles to all edges for debugging
    purposes. Calling this a graph instead of a 'bag of nodes' is misleading, because
    this object does not actually know anything about the connections between the
    various nodes.
    """

    def __init__(self, streams, operators):
        self.streams = streams
        self.operators = operators

    def step(self):
        for op in self.operators:
            op.run()
