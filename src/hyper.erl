%% @doc: Implementation of HyperLogLog with bias correction as
%% described in the Google paper,
%% http://static.googleusercontent.com/external_content/untrusted_dlcp/
%% research.google.com/en//pubs/archive/40671.pdf
-module(hyper).
%%-compile(native).
-include_lib("eunit/include/eunit.hrl").

-export([new/1, new/2, insert/2, card/1, union/1, union/2, intersect_card/2]).
-export([to_json/1, from_json/1, from_json/2, compact/1]).
-export([perf_report/0, bytes/1]).

-type precision() :: 4..16.
-type registers() :: any().

-record(hyper, {p :: precision(),
                registers :: {module(), registers()}}).

-type value()     :: binary().
-type filter()    :: #hyper{}.

-export_type([filter/0, precision/0, registers/0]).


%%
%% API
%%

-spec new(precision()) -> filter().
new(P) ->
    new(P, hyper_binary).

-spec new(precision(), module()) -> filter().
new(P, Mod) when 4 =< P andalso P =< 16 andalso is_atom(Mod) ->
    #hyper{p = P, registers = {Mod, Mod:new(P)}}.


-spec insert(value(), filter()) -> filter().
insert(Value, #hyper{registers = {Mod, Registers}, p = P} = Hyper)
  when is_binary(Value) ->
    Hash = crypto:hash(sha, Value),
    <<Index:P, RegisterValue:P/bitstring, _/bitstring>> = Hash,

    ZeroCount = run_of_zeroes(RegisterValue) + 1,

    case Mod:get(Index, Registers) of
        {ok, Small} when Small < ZeroCount ->
            Hyper#hyper{registers = {Mod, Mod:set(Index, ZeroCount, Registers)}};
        {ok, Large} when ZeroCount =< Large ->
            Hyper;
        undefined ->
            Hyper#hyper{registers = {Mod, Mod:set(Index, ZeroCount, Registers)}}
    end;

insert(_Value, _Hyper) ->
    error(badarg).


-spec union([filter()]) -> filter().
union(Filters) when is_list(Filters) ->
    %% Must have the same P and backend
    case lists:usort(lists:map(fun (#hyper{p = P, registers = {Mod, _}}) ->
                                       {P, Mod}
                               end, Filters)) of
        [{_P, Mod}] ->
            Registers = lists:map(fun (#hyper{registers = {_, R}}) ->
                                          R
                                  end, Filters),

            [First | _] = Filters,
            First#hyper{registers = {Mod, Mod:max_merge(Registers)}}
    end.

union(#hyper{registers = {Mod, SmallRegisters}} = Small,
      #hyper{registers = {Mod, BigRegisters}} = Big)
  when Small#hyper.p =:= Big#hyper.p ->
    NewRegisters = Mod:max_merge(SmallRegisters, BigRegisters),
    Big#hyper{registers = {Mod, NewRegisters}}.





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

bytes(#hyper{registers = {Mod, Registers}}) ->
    Mod:bytes(Registers).

compact(#hyper{registers = {Mod, Registers}} = Hyper) ->
    Hyper#hyper{registers = {Mod, Mod:compact(Registers)}}.

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
    from_json(Struct, hyper_gb).

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
    BiasVector = list_to_tuple(hyper_const:bias_data(P)),
    NearestNeighbours = nearest_neighbours(E, list_to_tuple(hyper_const:estimate_data(P))),
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
%% TESTS
%%


hyper_test_() ->
    {foreach, fun () -> ok end, fun (_) -> ok end,
     [
      ?_test(basic_t()),
      ?_test(serialization_t()),
      ?_test(backend_t()),
      ?_test(encoding_t()),
      ?_test(register_sum_t()),
      {timeout, 10, ?_test(error_range_t())},
      ?_test(many_union_t()),
      ?_test(union_t()),
      ?_test(small_big_union_t()),
      ?_test(intersect_card_t())
     ]}.

basic_t() ->
    lists:foreach(fun (Mod) ->
                     ?assertEqual(1, trunc(card(insert(<<"1">>, new(4, Mod)))))
             end, [hyper_bisect, hyper_binary, hyper_gb, hyper_array]).


serialization_t() ->
    Mod = hyper_binary,
    Hyper = compact(insert_many(generate_unique(10), new(5, Mod))),

    {hyper_binary, L} = Hyper#hyper.registers,
    {hyper_binary, R} = (compact(from_json(to_json(Hyper), Mod)))#hyper.registers,

    ?assertEqual(trunc(card(Hyper)), trunc(card(from_json(to_json(Hyper), Mod)))),
    ?assertEqual(Hyper#hyper.p, (from_json(to_json(Hyper), Mod))#hyper.p),
    ?assertEqual(L, R).


backend_t() ->
    Values = generate_unique(15),

    Gb     = compact(insert_many(Values, new(7, hyper_gb))),
    Array  = compact(insert_many(Values, new(7, hyper_array))),
    Bisect = compact(insert_many(Values, new(7, hyper_bisect))),
    Binary = compact(insert_many(Values, new(7, hyper_binary))),

    ?assertEqual(card(Gb), card(Array)),
    ?assertEqual(card(Gb), card(Bisect)),
    ?assertEqual(card(Gb), card(Binary)),

    {hyper_gb    , GbRegisters}     = Gb#hyper.registers,
    {hyper_array , ArrayRegisters}  = Array#hyper.registers,
    {hyper_bisect, BisectRegisters} = Bisect#hyper.registers,
    {hyper_binary, BinaryRegisters} = Binary#hyper.registers,

    %% error_logger:info_msg("Gb:     ~p~nArray:  ~p~nBisect: ~p~nBinary: ~p~n",
    %%                       [hyper_gb:encode_registers(GbRegisters),
    %%                        hyper_array:encode_registers(ArrayRegisters),
    %%                        hyper_bisect:encode_registers(BisectRegisters),
    %%                        hyper_binary:encode_registers(BinaryRegisters)]),

    ?assertEqual(hyper_gb:encode_registers(GbRegisters),
                 hyper_array:encode_registers(ArrayRegisters)),

    ?assertEqual(hyper_gb:encode_registers(GbRegisters),
                 hyper_bisect:encode_registers(BisectRegisters)),

    ?assertEqual(hyper_gb:encode_registers(GbRegisters),
                 hyper_binary:encode_registers(BinaryRegisters)),

    {_, {GbSerialized, _}} = (from_json(to_json(Array), hyper_gb))#hyper.registers,
    ?assertEqual(gb_trees:to_list(element(1, GbRegisters)),
                 gb_trees:to_list(GbSerialized)),

    ?assertEqual(card(Gb), card(from_json(to_json(Array), hyper_gb))),
    ?assertEqual(Array, from_json(to_json(Array), hyper_array)),
    ?assertEqual(Array, from_json(to_json(Bisect), hyper_array)),
    ?assertEqual(Bisect, from_json(to_json(Array), hyper_bisect)),

    ?assertEqual(to_json(Gb), to_json(Array)),
    ?assertEqual(to_json(Gb), to_json(Bisect)),
    ?assertEqual(to_json(Gb), to_json(Binary)).



encoding_t() ->
    Hyper = insert_many(generate_unique(10), new(4)),
    ?assertEqual(trunc(card(Hyper)), trunc(card(from_json(to_json(Hyper))))).


register_sum_t() ->
    Mods = [hyper_array, hyper_gb, hyper_bisect, hyper_binary],
    P = 4,
    M = trunc(math:pow(2, P)),

    SetRegisters = [1, 5, 10],
    RegisterValue = 3,

    ExpectedSum =
        (math:pow(2, -0) * M)
        - (math:pow(2, -0) * length(SetRegisters))
        + (math:pow(2, -RegisterValue) * length(SetRegisters)),

    [begin
         Registers = lists:foldl(fun (I, Acc) ->
                                         Mod:set(I, RegisterValue, Acc)
                                 end, Mod:new(P), SetRegisters),
         ?assertEqual({Mod, ExpectedSum}, {Mod, Mod:register_sum(Registers)})
     end || Mod <- Mods].


error_range_t() ->
    Mods = [hyper_gb, hyper_array, hyper_bisect, hyper_binary],
    Run = fun (Cardinality, P, Mod) ->
                  lists:foldl(fun (V, H) ->
                                      insert(V, H)
                              end, new(P, Mod), generate_unique(Cardinality))
          end,
    ExpectedError = 0.02,
    P = 14,
    random:seed(1, 2, 3),

    [begin
         Estimate = trunc(card(Run(Card, P, Mod))),
         ?assert(abs(Estimate - Card) < Card * ExpectedError)
     end || Card <- lists:seq(1000, 50000, 5000),
            Mod <- Mods].

many_union_t() ->
    random:seed(1, 2, 3),
    Card = 1000,
    NumSets = 3,

    Sets = [sets:from_list(generate_unique(Card)) || _ <- lists:seq(1, NumSets)],
    Filters = lists:map(fun (S) ->
                                insert_many(sets:to_list(S),
                                            new(10, hyper_bisect))
                        end, Sets),

    ?assert(abs(sets:size(sets:union(Sets)) - card(union(Filters)))
            < (Card * NumSets) * 0.1).



union_t() ->
    random:seed(1, 2, 3),

    LeftDistinct = sets:from_list(generate_unique(10000)),

    RightDistinct = sets:from_list(generate_unique(5000)
                                   ++ lists:sublist(sets:to_list(LeftDistinct),
                                                    5000)),

    LeftHyper = insert_many(sets:to_list(LeftDistinct), new(13)),
    RightHyper = insert_many(sets:to_list(RightDistinct), new(13)),

    UnionHyper = union(LeftHyper, RightHyper),
    Intersection = card(LeftHyper) + card(RightHyper) - card(UnionHyper),

    ?assert(abs(card(UnionHyper) - sets:size(sets:union(LeftDistinct, RightDistinct)))
            < 200),
    ?assert(abs(Intersection - sets:size(
                                 sets:intersection(LeftDistinct, RightDistinct)))
            < 200).

small_big_union_t() ->
    random:seed(1, 2, 3),
    SmallCard = 100,
    BigCard   = 15000, % switches to dense at 10922 items

    SmallSet = sets:from_list(generate_unique(SmallCard)),
    BigSet   = sets:from_list(generate_unique(BigCard)),

    SmallHyper = insert_many(sets:to_list(SmallSet), new(15, hyper_bisect)),
    BigHyper   = insert_many(sets:to_list(BigSet), new(15, hyper_bisect)),
    ?assertMatch({hyper_bisect, {sparse, _, _, _}}, SmallHyper#hyper.registers),
    ?assertMatch({hyper_bisect, {dense, _}}, BigHyper#hyper.registers),

    UnionHyper = union(SmallHyper, BigHyper),
    TrueUnion = sets:size(sets:union(SmallSet, BigSet)),
    ?assert(abs(card(UnionHyper) - TrueUnion) < TrueUnion * 0.01).



intersect_card_t() ->
    random:seed(1, 2, 3),

    LeftDistinct = sets:from_list(generate_unique(10000)),

    RightDistinct = sets:from_list(generate_unique(5000)
                                   ++ lists:sublist(sets:to_list(LeftDistinct),
                                                    5000)),

    LeftHyper = insert_many(sets:to_list(LeftDistinct), new(13)),
    RightHyper = insert_many(sets:to_list(RightDistinct), new(13)),

    IntersectCard = intersect_card(LeftHyper, RightHyper),

    ?assert(IntersectCard =< hyper:card(hyper:union(LeftHyper, RightHyper))),

    %% NOTE: we can't really say much about the error here,
    %% so just pick something and see if the intersection makes sense
    Error = 0.05,
    ?assert((abs(5000 - IntersectCard) / 5000) =< Error).


%% report_wrapper_test_() ->
%%     [{timeout, 600000000, ?_test(estimate_report())}].

estimate_report() ->
    random:seed(erlang:now()),
    Ps = lists:seq(10, 16, 1),
    Cardinalities = [100, 1000, 10000, 100000, 1000000],
    Repetitions = 50,

    %% Ps = [4, 5],
    %% Cardinalities = [100],
    %% Repetitions = 100,

    Stats = [run_report(P, Card, Repetitions) || P <- Ps,
                                                 Card <- Cardinalities],
    error_logger:info_msg("~p~n", [Stats]),

    Result =
        "p,card,mean,p99,p1,bytes~n" ++
        lists:map(fun ({P, Card, Mean, P99, P1, Bytes}) ->
                          io_lib:format("~p,~p,~.2f,~.2f,~.2f,~p~n",
                                        [P, Card, Mean, P99, P1, Bytes])
                  end, Stats),
    error_logger:info_msg(Result),
    ok = file:write_file("../data.csv", io_lib:format(Result, [])).

run_report(P, Card, Repetitions) ->
    Estimations = lists:map(fun (_) ->
                                    Elements = generate_unique(Card),
                                    abs(Card - card(insert_many(Elements, new(P))))
                            end, lists:seq(1, Repetitions)),
    error_logger:info_msg("p=~p, card=~p, reps=~p~nestimates=~p~n",
                          [P, Card, Repetitions, Estimations]),
    Hist = basho_stats_histogram:update_all(
             Estimations,
             basho_stats_histogram:new(
               0,
               lists:max(Estimations),
               length(Estimations))),


    {_, Mean, _, _, _} = basho_stats_histogram:summary_stats(Hist),
    P99 = basho_stats_histogram:quantile(0.99, Hist),
    P1 = basho_stats_histogram:quantile(0.01, Hist),

    {P, Card, Mean, P99, P1, trunc(pow(2, P))}.




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

insert_many(L, Hyper) ->
    lists:foldl(fun insert/2, Hyper, L).


%%
%% REPORTS
%%

perf_report() ->
    Ps    = [15],
    Cards = [1, 100, 1000, 5000, 10000, 15000, 25000, 50000, 100000, 1000000],
    Mods  = [hyper_gb, hyper_array, hyper_bisect, hyper_binary],
    Repeats = 1,

    random:seed(1, 2, 3),

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
             InsertUs = Time(fun (Values, H) ->
                                     insert_many(Values, H)
                             end,
                             [generate_unique(Card), new(P, Mod)]),

             UnionUs = Time(fun union/2,
                            [insert_many(generate_unique(Card div 10), new(P, Mod)),
                             insert_many(generate_unique(Card), new(P, Mod))]),

             CardUs = Time(fun card/1,
                           [insert_many(generate_unique(Card), new(P, Mod))]),

             ToJsonUs = Time(fun to_json/1,
                             [insert_many(generate_unique(Card), new(P, Mod))]),


             Filter = insert_many(generate_unique(Card), new(P, Mod)),
             {Mod, Registers} = Filter#hyper.registers,
             Fill = Mod:fold(fun (_, V, Acc) when V > 0 -> Acc+1;
                                 (_, _, Acc) -> Acc
                             end, 0, Registers),
             Bytes = bytes(Filter),

             {Mod, P, Card, Fill, Bytes,
              InsertUs / Card, UnionUs, CardUs, ToJsonUs}

         end || Mod  <- Mods,
                P    <- Ps,
                Card <- Cards],

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
                          M = trunc(math:pow(2, P)),
                          Filled = lists:flatten(io_lib:format("~.2f", [Fill / M])),

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
