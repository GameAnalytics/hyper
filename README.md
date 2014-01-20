# HyperLogLog for Erlang

This is an implementation of the HyperLogLog algorithm in
Erlang. Using HyperLogLog you can estimate the cardinality of very
large data sets using constant memory. The relative error is `1.04 *
sqrt(2^P)`. When creating a new HyperLogLog filter, you provide the
precision P, allowing you to trade memory for accuracy. The union of
two filters is lossless.

In practice this allows you to build efficient analytics systems. For
example, you can create a new filter in each mapper and feed it a
portion of your dataset while the reducers simply union together all
filters they receive. The filter you end up with is exactly the same
filter as if you would sequentially insert all data into a single
filter.

In addition to the base algorithm, we have implemented the bias
correction from HLL++ as the described in the excellent [paper by
Google][].


## Usage

```erlang
1> hyper:insert(<<"foobar">>, hyper:insert(<<"quux">>, hyper:new(4))).
{hyper,4,
       {hyper_binary,{dense,<<0,0,0,0,0,0,0,0,64,0,0,0>>,
                            [{8,1}],
                            1,16}}}

2> hyper:card(v(-1)).
2.136502281992361
```

The error from estimations can be seen in this example:
```erlang
3> random:seed(1,2,3).
undefined
4> Run = fun (P, Card) -> hyper:card(lists:foldl(fun (_, H) -> Int = random:uniform(10000000000000), hyper:insert(<<Int:64/integer>>, H) end, hyper:new(P), lists:seq(1, Card))) end.
#Fun<erl_eval.12.80484245>
5> Run(12, 10000).
9992.846462080579
6> Run(14, 10000).
10055.568563614219
7> Run(16, 10000).
10007.654167606248
```

A filter can be persisted and read later. The serialized struct is formatted for usage with jiffy:
```erlang
8> Filter1 = hyper:insert(<<"foo">>, hyper:new(4)).
{hyper,4,
       {hyper_binary,{dense,<<4,0,0,0,0,0,0,0,0,0,0,0>>,[],0,16}}}
9> Filter2 = hyper:from_json(hyper:to_json(Filter1)).
{hyper,4,
       {hyper_binary,{dense,<<4,0,0,0,0,0,0,0,0,0,0,0>>,[],0,16}}}
10> hyper:card(Filter1) =:= hyper:card(Filter2).
true
```

## Is it any good?

Yes. At Game Analytics we use it extensively.

## Backends

Effort has been spent on implementing different backends in the
pursuit of finding the right trade-off. A simple performance
comparison can be seen by running `make perf_report`. Fill rate refers
to how many registers has a value other than 0.

 * `hyper_binary`: Fixed memory usage (6 bits * 2^P), fastest on insert,
   union, cardinality and serialization. Best default choice.

 * hyper_bisect: Lower memory usage at lower fill rates (3 bytes per
   used entry), slightly slower than hyper_binary for
   everything. Switches to a structure similar to hyper_binary when it
   would save memory. Room for further optimization.

 * hyper_gb: Fast inserts, very fast unions and reasonable memory
   usage at low fill rates. Unreasonable memory usage at high fill
   rates.

 * hyper_array: Cardinality estimation is constant, but slower than
   hyper_gb for low fill rates. Uses much more memory at lower fill
   rates, but stays constant from 25% and upwards.

 * hyper_binary_rle: Dud

You can also implement your own backend. In `hyper_test` theres a
bunch of tests run for all backends, including some PropEr tests. The
test suite will ensure your backend gives correct estimates and
correctly encodes/decodes the serialized filters.



[paper by Google]: http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en//pubs/archive/40671.pdf
