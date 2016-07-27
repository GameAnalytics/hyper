%% @doc: Implementation of HyperLogLog with bias correction as
%% described in the Google paper,
%% http://static.googleusercontent.com/external_content/untrusted_dlcp/
%% research.google.com/en//pubs/archive/40671.pdf
-module(hyper).
%%-compile(native).

-export([new/1, new/2, insert/2, insert_many/2]).
-export([union/1, union/2]).
-export([card/1, intersect_card/2]).
-export([to_json/1, from_json/1, from_json/2, precision/1, bytes/1, is_hyper/1]).
-export([compact/1, reduce_precision/2]).

-type precision() :: 4..16.
-type registers() :: any().

-record(hyper, {p :: precision(),
                registers :: {module(), registers()}}).

-type value()     :: binary().
-type filter()    :: #hyper{}.

-export_type([filter/0, precision/0, registers/0]).

%% Exported for testing
-export([run_of_zeroes/1, perf_report/0, estimate_report/0]).

-define(DEFAULT_BACKEND, hyper_binary).

%%
%% API
%%

-spec new(precision()) -> filter().
new(P) ->
    new(P, ?DEFAULT_BACKEND).

-spec new(precision(), module()) -> filter().
new(P, Mod) when 4 =< P andalso P =< 16 andalso is_atom(Mod) ->
    #hyper{p = P, registers = {Mod, Mod:new(P)}}.

-spec is_hyper(filter()) -> boolean().
is_hyper(#hyper{}) -> true;
is_hyper(_) -> false.


-spec insert(value(), filter()) -> filter().
insert(Value, #hyper{registers = {Mod, Registers}, p = P} = Hyper)
  when is_binary(Value) ->
    Hash = crypto:hash(sha, Value),
    <<Index:P, RegisterValue:P/bitstring, _/bitstring>> = Hash,

    ZeroCount = run_of_zeroes(RegisterValue) + 1,

    %% Registers are only allowed to increase, implement by backend
    Hyper#hyper{registers = {Mod, Mod:set(Index, ZeroCount, Registers)}};

insert(_Value, _Hyper) ->
    error(badarg).

-spec insert_many([value()], filter()) -> filter().
insert_many(L, Hyper) ->
    lists:foldl(fun insert/2, Hyper, L).



-spec union([filter()]) -> filter().
union(Filters) when is_list(Filters) ->
    case lists:usort(lists:map(fun (#hyper{p = P, registers = {Mod, _}}) ->
                                       {P, Mod}
                               end, Filters)) of
        %% same P and backend
        [{_P, Mod}] ->
            Registers = lists:map(fun (#hyper{registers = {_, R}}) ->
                                          R
                                  end, Filters),

            [First | _] = Filters,
            First#hyper{registers = {Mod, Mod:max_merge(Registers)}};

        %% mixed P, but still must have same backend
        [{MinP, Mod} | _] ->
            FoldedFilters = lists:map(fun (#hyper{registers = {M, _}} = F)
                                        when M =:= Mod ->
                                          hyper:reduce_precision(MinP, F)
                                  end, Filters),
            union(FoldedFilters)
    end.

union(Small, Big) ->
    union([Small, Big]).





%% NOTE: use with caution, no guarantees on accuracy.
-spec intersect_card(filter(), filter()) -> float().
intersect_card(Left, Right) when Left#hyper.p =:= Right#hyper.p ->
    max(0.0, (card(Left) + card(Right)) - card(union(Left, Right))).


-spec card(filter()) -> float().
card(#hyper{registers = {Mod, Registers0}, p = P}) ->
    M = trunc(pow(2, P)),
    Registers = Mod:compact(Registers0),

    RegisterSum = Mod:register_sum(Registers),

    E = alpha(M) * pow(M, 2) / RegisterSum,
    Ep = case E =< 5 * M of
             true -> E - estimate_bias(E, P);
             false -> E
         end,

    V = Mod:zero_count(Registers),

    H = case V of
            0 ->
                Ep;
            _ ->
                M * math:log(M / V)
            end,

    case H =< hyper_const:threshold(P) of
        true ->
            H;
        false ->
            Ep
    end.

precision(#hyper{p = Precision}) ->
    Precision.

bytes(#hyper{registers = {Mod, Registers}}) ->
    Mod:bytes(Registers).

compact(#hyper{registers = {Mod, Registers}} = Hyper) ->
    Hyper#hyper{registers = {Mod, Mod:compact(Registers)}}.

reduce_precision(P, #hyper{p = OldP, registers = {Mod, Registers}} = Hyper)
  when P < OldP ->
    Hyper#hyper{p = P, registers = {Mod, Mod:reduce_precision(P, Registers)}};
reduce_precision(P, #hyper{p = P} = Filter) ->
    Filter.

%%
%% SERIALIZATION
%%

-spec to_json(filter()) -> any().
to_json(#hyper{p = P, registers = {Mod, Registers}}) ->
    Compact = Mod:compact(Registers),
    {[
      {<<"p">>, P},
      {<<"registers">>, base64:encode(
                          zlib:gzip(
                            Mod:encode_registers(Compact)))}
     ]}.

-spec from_json(any()) -> filter().
from_json(Struct) ->
    from_json(Struct, ?DEFAULT_BACKEND).

-spec from_json(any(), module()) -> filter().
from_json({Struct}, Mod) ->
    P = proplists:get_value(<<"p">>, Struct),
    Bytes = zlib:gunzip(
              base64:decode(
                proplists:get_value(<<"registers">>, Struct))),
    Registers = Mod:decode_registers(Bytes, P),

    #hyper{p = P, registers = {Mod, Registers}}.


%%
%% HELPERS
%%


alpha(16) -> 0.673;
alpha(32) -> 0.697;
alpha(64) -> 0.709;
alpha(M)  -> 0.7213 / (1 + 1.079 / M).

pow(X, Y) ->
    math:pow(X, Y).

run_of_zeroes(B) ->
    run_of_zeroes(1, B).

run_of_zeroes(I, B) ->
    case B of
        <<0:I, _/bitstring>> ->
            run_of_zeroes(I + 1, B);
        _ ->
            I - 1
    end.



estimate_bias(E, P) ->
    BiasVector = hyper_const:bias_data(P),
    EstimateVector = hyper_const:estimate_data(P),
    NearestNeighbours = nearest_neighbours(E, EstimateVector),

    lists:sum([element(Index, BiasVector) || Index <- NearestNeighbours])
        / length(NearestNeighbours).

nearest_neighbours(E, Vector) ->
    Distances = lists:map(fun (Index) ->
                                  V = element(Index, Vector),
                                  {pow((E - V), 2), Index}
                          end, lists:seq(1, size(Vector))),
    SortedDistances = lists:keysort(1, Distances),

    {_, Indexes} = lists:unzip(lists:sublist(SortedDistances, 6)),
    Indexes.




%%
%% REPORTS
%%


generate_unique(N) ->
    generate_unique(lists:usort(random_bytes(N)), N).

generate_unique(L, N) ->
    case length(L) of
        N ->
            L;
        Less ->
            generate_unique(lists:usort(random_bytes(N - Less) ++ L), N)
    end.


random_bytes(N) ->
    random_bytes([], N).

random_bytes(Acc, 0) -> Acc;
random_bytes(Acc, N) ->
    Int = random:uniform(100000000000000),
    random_bytes([<<Int:64/integer>> | Acc], N-1).



%% Lifted from berk, https://github.com/richcarl/berk/blob/master/berk.erl
median(Ns) ->
    N = length(Ns),
    Ss = lists:sort(Ns),
    if (N rem 2) > 0 ->
            lists:nth(1+trunc(N/2), Ss);
       true ->
            [X1,X2] = lists:sublist(Ss, trunc(N/2),2),
            (X1+X2)/2
    end.


estimate_report() ->
    Ps            = lists:seq(11, 16),
    Cardinalities = [100, 1000, 10000, 100000, 1000000],
    Repetitions   = 50,

    {ok, F} = file:open("estimates.csv", [write]),

    io:format(F, "p,card,median,p05,p95~n", []),

    [begin
         Stats = [run_report(P, Card, Repetitions) || Card <- Cardinalities],
         lists:map(fun ({Card, Median, P05, P95}) ->
                           io:format(F,
                                     "~p,~p,~p,~p,~p~n",
                                     [P, Card, Median, P05, P95])
                   end, Stats)
     end || P <- Ps],
    io:format("~n"),
    file:close(F).


run_report(P, Card, Repetitions) ->
    {ok, Estimations} = s2_par:map(
                          fun (I) ->
                                  io:format("~p values with p=~p, rep ~p~n",
                                            [Card, P, I]),
                                  random:seed(erlang:phash2([node()]),
                                              erlang:monotonic_time(),
                                              erlang:unique_integer()),
                                  Elements = generate_unique(Card),
                                  Estimate = card(insert_many(Elements, new(P))),
                                  abs(Card - Estimate) / Card
                          end,
                          lists:seq(1, Repetitions),
                          [{workers, 8}]),

    Hist = basho_stats_histogram:update_all(
             Estimations,
             basho_stats_histogram:new(
               0,
               lists:max(Estimations),
               length(Estimations))),

    P05 = basho_stats_histogram:quantile(0.05, Hist),
    P95 = basho_stats_histogram:quantile(0.95, Hist),
    {Card, median(Estimations), P05, P95}.


perf_report() ->
    Ps      = [15],
    Cards   = [1, 100, 500, 1000, 2500, 5000, 10000,
               15000, 25000, 50000, 100000, 1000000],
    Mods    = [hyper_gb, hyper_array, hyper_bisect, hyper_binary, hyper_carray],
    Repeats = 10,

    Time = fun (F, Args) ->
                   Run = fun () ->
                                 Parent = self(),
                                 Pid = spawn_link(
                                         fun () ->
                                                 {ElapsedUs, _} = timer:tc(F, Args),
                                                 Parent ! {self(), ElapsedUs}
                                         end),
                                 receive {Pid, ElapsedUs} -> ElapsedUs end
                         end,
                   lists:sum([Run() || _ <- lists:seq(1, Repeats)]) / Repeats
           end,


    R = [begin
             io:format("."),
             random:seed(1, 2, 3),

             M = trunc(math:pow(2, P)),
             InsertUs = Time(fun (Values, H) ->
                                     insert_many(Values, H)
                             end,
                             [generate_unique(Card), new(P, Mod)]),
             ReusableH = compact(insert_many(generate_unique(Card), new(P, Mod))),

             UnionUs = Time(fun (Fs) -> union(Fs) end,
                            [[insert_many(generate_unique(Card div 10), new(P, Mod)),
                              insert_many(generate_unique(Card div 10), new(P, Mod)),
                              insert_many(generate_unique(Card div 10), new(P, Mod)),
                              insert_many(generate_unique(Card div 10), new(P, Mod)),
                              insert_many(generate_unique(Card div 10), new(P, Mod))]]),

             CardUs = Time(fun card/1, [ReusableH]),

             ToJsonUs = Time(fun to_json/1, [ReusableH]),


             Filter = insert_many(generate_unique(Card), new(P, Mod)),

             {Mod, Registers} = Filter#hyper.registers,
             Bytes = Mod:encode_registers(Registers),
             Filled = lists:filter(fun (I) -> binary:at(Bytes, I) =/= 0 end,
                                   lists:seq(0, M-1)),

             {Mod, P, Card, length(Filled) / M, bytes(Filter),
              InsertUs / Card, UnionUs, CardUs, ToJsonUs}

         end || Mod  <- Mods,
                P    <- Ps,
                Card <- Cards],
    io:format("~n"),
    io:format("~s ~s ~s ~s ~s ~s ~s ~s ~s~n",
              [string:left("module"     , 12, $ ),
               string:left("P"          ,  4, $ ),
               string:right("card"      ,  8, $ ),
               string:right("fill"      ,  6, $ ),
               string:right("bytes"     , 10, $ ),
               string:right("insert us" , 10, $ ),
               string:right("union ms"  , 10, $ ),
               string:right("card ms"   , 10, $ ),
               string:right("json ms"   , 10, $ )
              ]),

    lists:foreach(fun ({Mod, P, Card, Fill, Bytes,
                        AvgInsertUs, AvgUnionUs, AvgCardUs, AvgToJsonUs}) ->
                          Filled = lists:flatten(io_lib:format("~.2f", [Fill])),

                          AvgInsertUsL = lists:flatten(
                                     io_lib:format("~.2f", [AvgInsertUs])),
                          UnionMs = lists:flatten(
                                      io_lib:format("~.2f", [AvgUnionUs / 1000])),
                          CardMs = lists:flatten(
                                     io_lib:format("~.2f", [AvgCardUs / 1000])),
                          ToJsonMs = lists:flatten(
                                       io_lib:format("~.2f", [AvgToJsonUs / 1000])),
                          io:format("~s ~s ~s ~s ~s ~s ~s ~s ~s~n",
                                    [
                                     string:left(atom_to_list(Mod)      , 12, $ ),
                                     string:left(integer_to_list(P)     ,  4, $ ),
                                     string:right(integer_to_list(Card) ,  8, $ ),
                                     string:right(Filled                ,  6, $ ),
                                     string:right(integer_to_list(Bytes), 10, $ ),
                                     string:right(AvgInsertUsL          , 10, $ ),
                                     string:right(UnionMs               , 10, $ ),
                                     string:right(CardMs                , 10, $ ),
                                     string:right(ToJsonMs              , 10, $ )
                                    ])
                  end, R).
