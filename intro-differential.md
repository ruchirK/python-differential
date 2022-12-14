Differential, from scratch.

Materialize is a streaming data warehouse that provides correct, low-latency answers to queries that need to be answered repeatedly over changing data by *incrementally maintaining answers* to those queries. Materialize uses Differential Dataflow as the computational core to incrementally maintain user requested queries. This post will attempt to explain Differential Dataflow by starting from scratch and reimplementing the key ideas used by Differential in Python. Differential is very carefully engineered to run efficiently across multiple threads, processes, and/or machines but I will skip all of that. I'll also skip all of (or as much as possible) the work that the Timely Dataflow layer does that's not essential to incrementally maintaining computations. Ideally this post will answer "what the heck is Differential Dataflow, what does it do, and why is that hard" for folks who have absolutely no familiarity with dataflow programming, Timely, or Rust, but they do know Python 3. All the code for this post lives in [this repository](LINK TODO).


Prior art in this domain includes: Frank's blog posts introducing differential and Jamie Brandon's dida library.

# Intro / What are we trying to compute?

We will represent data as multisets of immutable, arbitarily typed records, and we will call these multisets collections. I'll implement collections as a list of pairs of `(data, multiplicity)`, where `data` is any record, and `multiplicity` is an integer indicating how many times that record is present in the collection. So as an example:

```
[('cat', 4), ('dog', 2)]
```

is a collection with 4 instances of `'cat'` and 2 instances of `'dog'`.

```
[((2, 'prime'), 1), ((2, 'even'), 1), ((3, 'prime'), 1), ((3, 'odd'), 1) ((4, 'composite'), 1), ((4, 'even'), 1), ((5, 'prime'), 1), ((5, 'odd'), 1)]
```

is a collection (which also happens to be a set, as each record occurs once). Each record is a pair of `(int, str)` where the first element in the pair is an integer in [2, 5] and the second element is a string indicating whether the first element is even / odd, or prime / composite. Collections where the records are pairs have a special signficance sometimes, and the first element of the pair is called a key, and the second element is called a value. But just to reiterate, the records can be pairs, triples, etc it doesn't matter. 

The following collections are all logically equivalent, even though physically they are different.

```
[('cat', 4), ('dog', 2)]
[('cat', 1), ('cat', 3), ('dog', 2)]
[('dog', 2), ('cat', 4)]
[('cat', 4), ('dog', 2), ('elephant', 0)]
[('cat', 4), ('dog', 2), ('elephant', -4), ('elephant', 4)]
```

This is just a consequence of collections being multisets. Finally, the multiplicities in a collection can also be negative, so the following is also a valid collection.
```
[('apple', 2), ('banana', -2)]
```

We'll be working exclusively with collections, and applying functional operations to them. Each operation will take as input one or two collections and produce a new collection as output. I'll quickly summarize some of the operations, but everything is implemented in [collections.py](LINK TODO) which you can play around with online [here](LINK TODO).
  - Concat: add together two collections.
  - Map / Filter: apply a function `f` to all of the records in the collection and produce new collection containing `f(record)` / `record if f(record) == True`.
  - Reduce: This is the first example of a operation that requires key-value structure. For each key in the input collection, `reduce` applies a function `f` to the multiset of values associated with that key, and returns a collection containing `(key, f(all values associated with that key))`. There's a couple of operations built on top of `reduce` like `count` (return the total number of values associated with each key), `sum` (return the sum of all of the values associated with a key). There's a few others in the linked file above.
  - Join: Takes two input collections, and for all `(x, y)` in the first collection, and all `(x, z) in the second collection, it produces `(x, (y, z))` as output.
  - Iterate: This operation is maybe the most surprising for most folks. `iterate` takes one input collection and repeatedly applies a function `f` to the input until the result stops changing. `f` can be any combination of the functional operations defined above, including other, nested calls to `iterate`.

These functional operations (and a few more) are the verbs in Differential. All computations have to be expressed as a combination of some input collection(s) + some combination of operations applied to the input(s). And then the output for all computations is an output collection. As an example, we could have the following silly computation that takes a collection of numbers, continues incrementing them and adding new numbers to the collection up to 5, and then prints the resulting `(number, number^2)`. This is just a demo of how all of the pieces fit together and not an interesting computation in itself. We can define the computation like so:

```
    def add_one(collection):
        return collection.map(lambda data: data + 1)  \
            .concat(collection)                       \
            .filter(lambda data: data <= 5)          \
            .map(lambda data: (data, ()))             \
            .distinct()                               \
            .map(lambda data: data[0])                \
            .consolidate()
    
    collection = Collection([(1, 1)])
    print(collection.iterate(add_one).map(lambda data: (data, data * data)))
```

And when you run this, you get, as expected.:

```
Collection([((1, 1), 1), ((2, 4), 1), ((3, 9), 1), ((4, 16), 1), ((5, 25), 1)]
```

The special/interesting/novel/cool thing that Differential does is that it supports efficiently updating the output collection as the inputs change, and doesn't have to fully recompute the output, even on computations with `iterate`. "Efficiently" here roughly means "taking time proportional to the magnitude of the change in inputs * logarithmic factors on other stuff". Differential also does all of this interactively, in that the inputs can be updated as computation is ongoing. We'll get to all of this, but we're going to work up to it.

## Incremental computation

So far, we've set up some machinery to define some computation `f` as a composition of functional operations, and we know that if we feed in an input
collection to `f`, we'll get an output collection out. We'll now allow for sequences of collections, where each collection in the sequence has a unique version. For now, we will represent traces as lists of pairs of `(version, collection)` where versions are nonnegative integers. Sometimes the versions are also called times/timestamps. I think both names are fine but one important difference between versions and time in everyday life is - everything that is versioned in day to day life got assigned that version because someone somewhere chose a version number or something for it, but often for events the time at which they occur is not the result of a conscious choice, but rather part of the description of the event. In Differential (and also in Materialize), versions/times are a conscious choice about how we want to label some data, and not a description of when the data occurred.

So if `A`, `B` and `C` are various collections, an example collection trace could be something like:

```
input_trace = [(0, A), (1, B), (2, C)]
```
Now, if we perform some computation `f` on this `input\_trace`, we'd like to get back an output trace that contains:

```
output_trace = [(0, f(A)), (1, f(B)), (2, f(C))]
```

Representing traces with lists of pairs instead of some other data structure like dicts isn't important, and neither is the actual order or physical layout of the list. For example we could have an equivalent input trace that looks like this:

```
equivalent_input_trace = [(0, A0), (1 B), (2, C), (0, A1)]
```

which would be logically equivalent to the first `input\_trace iff `A0 + A1 == A`. In fact, this case foreshadows what will happen when we move beyond performing computations on predetermined traces, and have to explicitly be told that a particular time has no more information incoming. For now, the only important takeaway is that the trace represents a mapping between versions <-> and for now, the versions are restricted to be nonnegative integers.

In order to perform computation efficiently, we'll switch from looking at actual input/output collections at each version to looking at input/output differences between successive versions. We can define the difference collection at version i as (pseudocode):

```
difference(collection_trace, i) = if i == 0: collection_trace.get_version(i) else: collection_trace.get_version(i) - collection_trace.get_version(i - 1)
```

The difference between two collections is also itself a collection (thanks, negative multiplicities), so the difference trace of a given collection trace is just a trace that maps `version -> difference(collection\_trace, version)`. The difference traces for the example inputs and outputs above would be:

```
input_difference_trace = [(0, A), (1, B - A), (2, C - B)]
output_difference_trace = [(0, f(A)), (1, f(B) - f(A)), (2, f(C) - f(B))]
```

And we can go from a difference trace back to the actual collection at a given version `i` by adding up the differences for all of the versions <= `i`. 

Working in terms of differences can lead to two distinct, but related benefits if the differences between successive versions is small:

  1. Some functions have additional properties that let us generate the output difference from a given input difference without having to look at the previously accumulated output. This helps us generate the output difference trace fast, and helps us avoid extra recomputation.
  2. Additionally, if we store only the differences between successive versions, we can store many versions worth of data while using much less space than we would if we stored each each collection separately. This is important perhaps if we need to retain the knowledge of many prior versions.

But again, 1. and 2. are only beneficial to us if the differences are small / successive versions are similar to each other. If the input collection trace consists of a sequence of collections that are all entirely different from each other, there's unfortunately nothing magical that anyone can do to compute the output trace other than computing outputs from scratch at each version. However, we're going to stay focused on the case where the differences between successive versions is small.

As a starting point to building up Differential we will first translate all of the methods on `Collection` above, to work on a `CollectionTrace` which is just a finite difference trace, and change all of the methods to produce a finite difference trace as output.

Some of the operations (map, filter, concat, negate) are linear. This means that they can get away with just operating on each difference collection as it comes, and don't need to hold onto any additional state.

