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
correction from HLL++ as the described in the excellent
[paper by Google][]. Bias correction greatly improves the estimates
for lower cardinalities.


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

The errors introduced by estimations can be seen in this example:
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
8> Filter = hyper:insert(<<"foo">>, hyper:new(4)).
{hyper,4,
       {hyper_binary,{dense,<<4,0,0,0,0,0,0,0,0,0,0,0>>,[],0,16}}}
9> Filter =:= hyper:from_json(hyper:to_json(Filter)).
true
```

You can select a different backend. See below for a description of why
you might want to do so. They serialize in exactly the same way, but
can't be mixed in memory.

```erlang
1> Gb = hyper:insert(<<"foo">>, hyper:new(4, hyper_gb)).
{hyper,4,{hyper_gb,{{1,{0,1,nil,nil}},16}}}
2> B = hyper:insert(<<"foo">>, hyper:new(4, hyper_binary)).
{hyper,4,
       {hyper_binary,{dense,<<4,0,0,0,0,0,0,0,0,0,0,0>>,[],0,16}}}
3> hyper:to_json(Gb) =:= hyper:to_json(B).
true
4> hyper:union(Gb, B).
** exception error: no case clause matching [{4,hyper_binary},{4,hyper_gb}]
     in function  hyper:union/1 (src/hyper.erl, line 65)
```


## Is it any good?

Yes. At Game Analytics we use it extensively.

## Backends

Effort has been spent on implementing different backends in the
pursuit of finding the right performance trade-off. The estimate will
always be the same, regardless of backend. A simple performance
comparison can be seen by running `make perf_report`, see below for
the results from an i7-3770 at 3.4 GHz. Fill rate refers to how many
registers has a value other than 0.

 * `hyper_binary`: Fixed memory usage (6 bits * 2^P), fastest on insert,
   union, cardinality and serialization. Best default choice.

 * `hyper_bisect`: Lower memory usage at lower fill rates (3 bytes per
   used entry), slightly slower than hyper_binary for
   everything. Switches to a structure similar to hyper_binary when it
   would save memory. Room for further optimization.

 * `hyper_gb`: Fast inserts, very fast unions and reasonable memory
   usage at low fill rates. Unreasonable memory usage at high fill
   rates.

 * `hyper_array`: Cardinality estimation is constant, but slower than
   hyper_gb for low fill rates. Uses much more memory at lower fill
   rates, but stays constant from 25% and upwards.

 * `hyper_binary_rle`: Dud

You can also implement your own backend. In `hyper_test` theres a
bunch of tests run for all backends, including some PropEr tests. The
test suite will ensure your backend gives correct estimates and
correctly encodes/decodes the serialized filters.



```
$ make perf_report
...

module       P        card   fill      bytes  insert us   union ms    card ms    json ms
hyper_gb     15          1   0.00         64     301.90       0.00       0.10       2.69
hyper_gb     15        100   0.00       3984       1.34       0.05       0.05       6.13
hyper_gb     15        500   0.02      19784       1.53       0.27       0.12       8.67
hyper_gb     15       1000   0.03      39384       1.72       0.53       0.19       8.67
hyper_gb     15       2500   0.07      96384       1.84       1.49       0.40      10.67
hyper_gb     15       5000   0.14     185224       1.99       3.24       0.71      12.22
hyper_gb     15      10000   0.26     344464       2.10       6.80       1.31      15.09
hyper_gb     15      15000   0.37     481664       2.02      10.07       1.91      17.99
hyper_gb     15      25000   0.53     698344       2.08      16.42       2.67      18.71
hyper_gb     15      50000   0.78    1027504       1.96      31.49       4.29      18.87
hyper_gb     15     100000   0.95    1248144       1.78      49.90       5.20      17.92
hyper_gb     15    1000000   1.00    1310744       1.10     108.38       5.72      18.96
hyper_array  15          1   0.00        520       4.10       0.00       4.74       3.83
hyper_array  15        100   0.00      19536       1.56       0.10       4.71       3.99
hyper_array  15        500   0.02      69328       1.44       0.43       4.63       4.46
hyper_array  15       1000   0.03     107760       1.51       0.79       4.68       4.29
hyper_array  15       2500   0.07     188384       1.35       1.79       4.70       5.16
hyper_array  15       5000   0.14     261520       1.31       3.27       4.72       5.33
hyper_array  15      10000   0.26     308072       1.21       5.45       4.99       6.90
hyper_array  15      15000   0.37     320128       1.22       7.34       4.96       7.72
hyper_array  15      25000   0.53     323384       1.21      11.07       5.18       8.42
hyper_array  15      50000   0.78     323560       1.04      17.90       5.51       8.08
hyper_array  15     100000   0.95     323560       1.00      26.93       5.60       7.70
hyper_array  15    1000000   1.00     323560       0.72      51.65       5.77       7.77
hyper_bisect 15          1   0.00          3       6.10       0.00       0.05       7.26
hyper_bisect 15        100   0.00        297       1.62       0.08       0.12      17.42
hyper_bisect 15        500   0.02       1482       1.72       0.42       0.59      21.30
hyper_bisect 15       1000   0.03       2952       1.83       0.95       1.10      23.21
hyper_bisect 15       2500   0.07       7227       1.97       2.63       2.53      25.91
hyper_bisect 15       5000   0.14      13890       2.23       6.50       5.06      28.80
hyper_bisect 15      10000   0.26      25833       2.74      18.21       9.18      31.61
hyper_bisect 15      15000   0.37      32768       3.67      33.29       4.40       4.46
hyper_bisect 15      25000   0.53      32768       3.11      78.77       4.60       5.17
hyper_bisect 15      50000   0.78      32768       2.53     190.66       4.90       4.89
hyper_bisect 15     100000   0.95      32768       2.05     381.11       5.07       4.43
hyper_bisect 15    1000000   1.00      32768       0.85      27.17       5.65       4.59
hyper_binary 15          1   0.00         88       3.40       0.00       6.06       2.23
hyper_binary 15        100   0.00       4048       0.63       0.01       5.91       2.38
hyper_binary 15        500   0.02      20048       0.50       0.01       5.67       2.58
hyper_binary 15       1000   0.02      24576       2.13       0.00       2.72       1.33
hyper_binary 15       2500   0.07      24576       1.54       1.97       2.72       1.95
hyper_binary 15       5000   0.12      24576       1.23       2.40       2.71       2.77
hyper_binary 15      10000   0.26      24576       1.10      11.16       2.95       4.46
hyper_binary 15      15000   0.34      24576       1.11      12.30       2.75       5.48
hyper_binary 15      25000   0.50      24576       0.95      11.97       2.79       5.83
hyper_binary 15      50000   0.76      24576       0.92      13.55       2.81       5.65
hyper_binary 15     100000   0.95      24576       0.79      11.74       2.59       5.16
hyper_binary 15    1000000   1.00      24576       0.55      13.88       2.64       5.11
```

[paper by Google]: http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en//pubs/archive/40671.pdf
