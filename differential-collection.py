from collections import defaultdict

def collection_consolidate(a):
    consolidated = defaultdict(int)
    for (data, diff) in a:
        consolidated[data] += diff
    return [(data, diff) for (data, diff) in consolidated.items() if diff != 0]

def collection_concat(a, b):
    out = []
    out.extend(a)
    out.extend(b)
    return collection_consolidate(out)

def collection_join(a, b):
   out = []
   for ((k1, v1), d1) in a:
       for ((k2, v2), d2) in b:
           if k1 == k2:
               out.append(((k1, (v1, v2)), d1 * d2))
   return collection_consolidate(out)

def collection_map(a, f):
    return [(f(data), diff) for (data, diff) in a]

def collection_filter(a, f):
    return [(data, diff) for (data, diff) in a if f(data) == True]

def collection_reduce(a, f):
    keys = defaultdict(list)
    out = []
    for ((key, val), diff) in a:
        keys[key].append((val, diff))
    for (key, vals) in keys.items():
        results = f(vals)
        for (val, diff) in results:
            out.append(((key, val), diff))
    return collection_consolidate(out)

def collection_count(a):
    def count(vals):
        out = 0
        for (_, diff) in vals:
            out += diff

        if out != 0:
            return [(out, 1)]
        else:
            return []

    return collection_reduce(a, count)

def collection_sum(a):
    def sum_inner(vals):
        out = 0
        for (val, diff) in vals:
            out += (val * diff)

        if out != 0:
            return [(out, 1)]
        else:
            return []

    return collection_reduce(a, sum_inner)

def collection_min(a):
    def min_inner(vals):
        out = vals[0][0]
        for (val, diff) in vals:
            assert(diff > 0)
            if val < out:
                out = val
        return [(out, 1)]
    return collection_reduce(a, min_inner)

def collection_max(a):
    def max_inner(vals):
        out = vals[0][0]
        for (val, diff) in vals:
            assert(diff > 0)
            if val > out:
                out = val
        return [(out, 1)]
    return collection_reduce(a, max_inner)

def collection_distinct(a):
    def distinct(vals):
        v = set()
        for (val, diff) in vals:
            assert(diff > 0)
            v.add(val)

        out = [(val, 1) for val in v]
        return out
    return collection_reduce(a, distinct)

def collection_iterate(a, f):
    curr = a
    while True:
        next_a = f(curr)
        if next_a == curr:
            break
        curr = next_a

    return curr

a = [(("apple", "$5"), 2), (("banana", "$2"), 1)]
b = [(("apple", "$3"), 1), (("apple", ("granny smith", "$2")), 1), (("kiwi", "$2"), 1)]

print(collection_concat(a, b))
print(collection_join(a, b))
print(collection_filter(a, lambda data: data[0] != "apple"))
print(collection_map(a, lambda data: (data[1], data[0])))
print(collection_count(collection_concat(a, b)))
print(collection_distinct(collection_concat(a, b)))

c = [(("apple", "$5"), 2), (("banana", "$2"), 1), (("apple", "$2"), 20)]
print(collection_min(c))
print(collection_max(c))

d = [(("apple", 11), 1), (("apple", 3), 2), (("banana", 2), 3), (("coconut", 3), 1)]
print(collection_sum(d))

def incremental_map(deltas, f):
    return [(t, collection_map(delta, f)) for (t, delta) in deltas]

def incremental_filter(deltas, f):
    return [(t, collection_filter(delta, f)) for (t, delta) in deltas]

trace = [(0, a), (1, [(("apple", "$5"), -1), (("apple", "$7"), 1)]), (3, [(("lemon", "$1"), 1)])]

print(incremental_map(trace, lambda data: (data[1], data[0])))
print(incremental_filter(trace, lambda data: data[0] != "apple"))

def incremental_concat(deltas_a, deltas_b):
    out = []
    out.extend(deltas_a)
    out.extend(deltas_b)
    return out

def incremental_join(deltas_a, deltas_b):
    trace_a = defaultdict(list)
    trace_b = defaultdict(list)
    trace_out = {}
    times = set()

    for (time, delta) in deltas_a:
        trace_a[time].extend(delta)
        times.add(time)
    for (time, delta) in deltas_b:
        trace_b[time].extend(delta)
        times.add(time)

    times = [time for time in times]
    times.sort()

    curr_a = []
    curr_b = []
    for time in times:
        delta_a = trace_a[time]
        delta_b = trace_b[time]

        a_delta_b = collection_join(curr_a, delta_b)
        b_delta_a = collection_join(delta_a, curr_b)
        delta_a_delta_b = collection_join(delta_a, delta_b)

        result = collection_concat(a_delta_b, collection_concat(b_delta_a, delta_a_delta_b))
        trace_out[time] = result
        curr_a = collection_concat(curr_a, delta_a)
        curr_b = collection_concat(curr_b, delta_b)
    return [(time, delta) for (time, delta) in trace_out.items()]

deltas_a = [(0, a)]
deltas_b = [(1, b)]

print(incremental_join(deltas_a, deltas_b))

def collection_sub(a, b):
   out = []
   out.extend(a)
   for (data, diff) in b:
      out.append((data, -diff))
   return collection_consolidate(out)

# TODO: there are simpler and more efficient impls for sum/count and I think
# distinct but I will skip those for now.
def incremental_reduce(deltas, f):
    trace_a = defaultdict(list)
    trace_out = {}
    times = set()

    for (time, delta) in deltas:
        trace_a[time].extend(delta)
        times.add(time)
   
    times = [time for time in times]
    times.sort()

    curr_a = []
    curr_out = []

    for time in times:
        delta_a = trace_a[time]
        next_a = collection_concat(curr_a, delta_a)
        result = f(next_a)
        delta_result = collection_sub(result, curr_out)
        trace_out[time] = delta_result
        curr_a = next_a
        curr_out = result
    return [(time, delta) for (time, delta) in trace_out.items()]

deltas = [(0, a), (1, [(("apple", "$5"), -1), (("apple", "$7"), 1)]), (3, [(("lemon", "$1"), 1)])]
print(incremental_reduce(deltas, collection_max))      
print(incremental_reduce(deltas, collection_min))  
print(incremental_reduce(deltas, collection_distinct))

from collections import deque
class Stream:
    def __init__(self):
        self.queues = []
        self.frozen = False

    def connect_to(self):
        assert(self.frozen == False)
        q = deque()
        self.queues.append(q)
        return q
    def send(self, data):
        self.frozen = True
        assert(len(self.queues) > 0)
        for q in self.queues:
            q.appendleft(data)

class LinearUnaryOperator:
    def __init__(self, input_a, f, min_time):
        self.input = input_a
        self.output = Stream()
        self.f = f
        self.input_frontier = min_time
        self.output_frontier = min_time

    def get_output(self):
        return self.output

    def run(self):
        while len(self.input) > 0:
            (typ, time, data) = self.input.pop()
            assert(time >= self.input_frontier)
            if typ == "data":
                result = self.f(data)
                self.output.send((typ, time, result))
            elif typ == "notify":
                self.input_frontier = time

        if self.input_frontier > self.output_frontier:
            self.output_frontier = self.input_frontier
            self.output.send(("notify", self.output_frontier, []))

class MapOperator(LinearUnaryOperator):
    def __init__(self, input_a, f, min_time):
        def map_inner(a):
            return collection_map(a, f)
        super().__init__(input_a, map_inner, min_time)

class FilterOperator(LinearUnaryOperator):
    def __init__(self, input_a, f, min_time):
        def filter_inner(a):
            return collection_filter(a, f)
        super().__init__(input_a, filter_inner, min_time)

class ConcatOperator:
    def __init__(self, input_a, input_b, min_time):
        self.input_a = input_a
        self.input_b = input_b
        self.output = Stream()
        self.input_a_frontier = min_time
        self.input_b_frontier = min_time
        self.output_frontier = min_time

    def get_output(self):
        return self.output

    def run(self):
        while len(self.input_a) > 0:
            (typ, time, data) = self.input_a.pop()
            assert(time >= self.input_a_frontier)
            if typ == "data":
                self.output.send((typ, time, data))
            elif typ == "notify":
                self.input_a_frontier = time
        while len(self.input_b) > 0:
            (typ, time, data) = self.input_b.pop()
            assert(time >= self.input_b_frontier)
            if typ == "data":
                self.output.send((typ, time, data))
            elif typ == "notify":
                self.input_b_frontier = time
        
        min_input_frontier = min(self.input_a_frontier, self.input_b_frontier)
        if min_input_frontier > self.output_frontier:
            self.output_frontier = min_input_frontier
            self.output.send(("notify", self.output_frontier, []))

input_a = Stream()
map_operator = MapOperator(input_a.connect_to(), lambda data: (data[1], data[0]), 0)
map_output = map_operator.get_output()
filter_operator = FilterOperator(input_a.connect_to(), lambda data: data[0] != "apple", 0)
filter_output = filter_operator.get_output()
concat_operator = ConcatOperator(map_output.connect_to(), filter_output.connect_to(), 0)
total_output = concat_operator.get_output().connect_to()
input_a.send(("data", 0, a))
print(total_output)
filter_operator.run()
map_operator.run()
print(total_output)
print(filter_output)
print(map_output)
concat_operator.run()
print(total_output)
print(filter_output)

class JoinOperator:
    def __init__(self, input_a, input_b, min_time):
        self.input_a = input_a
        self.input_b = input_b
        self.input_a_frontier = min_time
        self.input_b_frontier = min_time
        self.trace_a = defaultdict(list)
        self.trace_b = defaultdict(list)
        self.curr_a = []
        self.curr_b = []
        self.pending_times = []

        self.output = Stream()
        self.trace_out = defaultdict(list)
        self.output_frontier = min_time

    def get_output(self):
        return self.output

    def run(self):
        while len(self.input_a) > 0:
            (typ, time, data) = self.input_a.pop()
            assert(time >= self.input_a_frontier)
            if typ == "data":
                self.trace_a[time].extend(data)
                if time not in self.pending_times:
                    bisect.insort(self.pending_times, time)
            elif typ == "notify":
                self.input_a_frontier = time
        while len(self.input_b) > 0:
            (typ, time, data) = self.input_b.pop()
            assert(time >= self.input_b_frontier)
            if typ == "data":
                self.trace_b[time].extend(data)
                if time not in self.pending_times:
                    bisect.insort(self.pending_times, time)
            elif typ == "notify":
                self.input_b_frontier = time
        if min_input_frontier > self.output_frontier and len(self.pending_times) > 0 and min_input_frontier > self.pending_times[0]:
            time = self.pending_times[0]
            assert(time > self.output_frontier)
            delta_a = self.trace_a[time]
            delta_b = self.trace_b[time]
            a_delta_b = collection_join(curr_a, delta_b)
            b_delta_a = collection_join(delta_a, curr_b)
            delta_a_delta_b = collection_join(delta_a, delta_b)

            result = collection_concat(a_delta_b, collection_concat(b_delta_a, delta_a_delta_b))
            trace_out[time] = result
            curr_a = collection_concat(curr_a, delta_a)
            curr_b = collection_concat(curr_b, delta_b)
            self.output.send(("data", time, result))
            self.output_frontier = time
            self.output.send(("notify", time, []))

            
        

