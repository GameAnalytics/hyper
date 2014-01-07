# HyperLogLog for Erlang

`hyper` is an implementation of the HyperLogLog algorithm with bias
correction from HLL++ as (described by
Google)[http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en//pubs/archive/40671.pdf].

Using `hyper` you can estimate the cardinality of large data sets
using constant memory. The union of two HLL filters is lossless, which
allows perfect parallellization.

## Usage

```erlang
1> hyper:insert(<<"foobar">>, hyper:insert(<<"quux">>, hyper:new(4))).
{hyper,4,{hyper_gb,{2,{10,1,{8,1,nil,nil},nil}}}}
2> hyper:card(v(-1)).
2.136502281992361
```

The error from estimations can be seen in this example:
```erlang
10> Run = fun (P, Card) -> hyper:card(lists:foldl(fun (_, H) -> hyper:insert(crypto:rand_bytes(32), H) end, hyper:new(P), lists:seq(1, Card))) end.
#Fun<erl_eval.12.80484245>
11> Run(12, 10000).
9984.777312630407
12> Run(14, 10000).
9994.719695068114
13> Run(16, 10000).
9991.34646427657
```

## Report
