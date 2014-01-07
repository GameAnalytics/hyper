%% @doc: Implementation of HyperLogLog with bias correction as
%% described in the Google paper,
%% http://static.googleusercontent.com/external_content/untrusted_dlcp/
%% research.google.com/en//pubs/archive/40671.pdf
-module(hyper).
%%-compile(native).
-include_lib("eunit/include/eunit.hrl").

-export([new/1, new/2, insert/2, card/1, union/1, union/2, intersect_card/2]).
-export([to_json/1, from_json/1]).
-export([perf_report/0]).

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
    new(P, hyper_gb).

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
        [{_P, _Mod}] ->
            [First | Rest] = Filters,
            lists:foldl(fun union/2, First, Rest)
    end.

union(#hyper{registers = {Mod, LeftRegisters}} = Left,
      #hyper{registers = {Mod, RightRegisters}} = Right)
  when Left#hyper.p =:= Right#hyper.p ->
    NewRegisters = Mod:max_merge(LeftRegisters, RightRegisters),
    Right#hyper{registers = {Mod, NewRegisters}}.





%% NOTE: use with caution, no guarantees on accuracy.
-spec intersect_card(filter(), filter()) -> float().
intersect_card(Left, Right) when Left#hyper.p =:= Right#hyper.p ->
    max(0.0, (card(Left) + card(Right)) - card(union(Left, Right))).


-spec card(filter()) -> float().
card(#hyper{registers = {Mod, Registers}, p = P}) ->
    M = trunc(pow(2, P)),

    RegisterSum = register_sum(M-1, Mod, Registers, 0),

    E = alpha(M) * pow(M, 2) / RegisterSum,
    Ep = case E =< 5 * M of
             true -> E - estimate_bias(E, P);
             false -> E
         end,

    V = count_zeros(M-1, Mod, Registers, 0),

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

%%
%% SERIALIZATION
%%

-spec to_json(filter()) -> any().
to_json(Hyper) ->
    M = trunc(pow(2, Hyper#hyper.p)),
    {[
      {<<"p">>, Hyper#hyper.p},
      {<<"registers">>, encode_registers(M, Hyper#hyper.registers)}
     ]}.

-spec from_json(any()) -> filter().
from_json(Struct) ->
    from_json(Struct, hyper_gb).

-spec from_json(any(), module()) -> filter().
from_json({Struct}, Mod) ->
    P = proplists:get_value(<<"p">>, Struct),
    {_, Registers} = lists:foldl(fun (0, {I, A}) ->
                                         {I+1, A};
                                     (V, {I, A}) ->
                                         {I+1, Mod:set(I, V, A)}
                                 end,
                                 {0, Mod:new(P)},
                                 decode_registers(
                                   proplists:get_value(<<"registers">>, Struct))),

    #hyper{p = P, registers = {Mod, Registers}}.


encode_registers(M, Registers) ->
    ByteEncoded = encode_registers(M-1, Registers, []),
    base64:encode(
      zlib:gzip(
        iolist_to_binary(ByteEncoded))).

encode_registers(I, _Registers, ByteEncoded) when I < 0 ->
    ByteEncoded;

encode_registers(I, {Mod, Registers}, ByteEncoded) when I >= 0 ->
    Byte = case Mod:get(I, Registers) of
               {ok, V} ->
                   <<V:8/integer>>;
               undefined ->
                   <<0>>
           end,
    encode_registers(I - 1, {Mod, Registers}, [Byte | ByteEncoded]).


decode_registers(B) ->
    ByteEncoded = zlib:gunzip(base64:decode(B)),
    decode_registers(ByteEncoded, []).

decode_registers(<<>>, Acc) ->
    lists:reverse(Acc);
decode_registers(<<I:8/integer, Rest/binary>>, Acc) ->
    decode_registers(Rest, [I | Acc]).

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


register_sum(I, _Mod, _Registers, Sum) when I < 0 ->
    Sum;
register_sum(I, Mod, Registers, Sum) when I >= 0 ->
    Val = case Mod:get(I, Registers) of
              {ok, R} ->
                  pow(2, -R);
              undefined ->
                  pow(2, -0)
          end,
    register_sum(I - 1, Mod, Registers, Sum + Val).

count_zeros(I, _Mod, _Registers, Count) when I < 0 ->
    Count;
count_zeros(I, Mod, Registers, Count) when I >= 0 ->
    Val = case Mod:get(I, Registers) =:= undefined of
              true  -> 1;
              false -> 0
          end,
    count_zeros(I - 1, Mod, Registers, Count + Val).



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


basic_test() ->
    ?assertEqual(1, trunc(card(insert(<<"1">>, new(4, hyper_bisect))))),
    ?assertEqual(1, trunc(card(insert(<<"1">>, new(4, hyper_gb))))),
    ?assertEqual(1, trunc(card(insert(<<"1">>, new(4, hyper_array))))).


serialization_test() ->
    Hyper = insert_many(generate_unique(10), new(14, hyper_gb)),

    {hyper_gb, L} = Hyper#hyper.registers,
    {hyper_gb, R} = (from_json(to_json(Hyper)))#hyper.registers,

    ?assertEqual(trunc(card(Hyper)), trunc(card(from_json(to_json(Hyper))))),
    ?assertEqual(Hyper#hyper.p, (from_json(to_json(Hyper)))#hyper.p),
    ?assertEqual(gb_trees:to_list(L), gb_trees:to_list(R)).

encoding_test() ->
    Hyper = insert_many(generate_unique(100000), new(14)),
    ?assertEqual(trunc(card(Hyper)), trunc(card(from_json(to_json(Hyper))))).


error_range_test() ->
    Run = fun (Cardinality, P) ->
                  lists:foldl(fun (V, H) ->
                                      insert(V, H)
                              end, new(P), generate_unique(Cardinality))
          end,
    ExpectedError = 0.02,
    P = 14,
    lists:foreach(fun (Card) ->
                          Estimate = trunc(card(Run(Card, P))),
                          ?assert(abs(Estimate - Card) < Card * ExpectedError)
                  end, lists:seq(10000, 50000, 10000)).

many_union_test() ->
    random:seed(1, 2, 3),
    Card = 100000,
    NumSets = 3,

    Sets = [sets:from_list(generate_unique(Card)) || _ <- lists:seq(1, NumSets)],
    Filters = lists:map(fun (S) ->
                                insert_many(sets:to_list(S),
                                            new(10, hyper_bisect))
                        end, Sets),

    ?assert(abs(sets:size(sets:union(Sets)) - card(union(Filters)))
            < (Card * NumSets) * 0.1).



union_test() ->
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

intersect_card_test() ->
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


    {_, Mean, _, _, Sd} = basho_stats_histogram:summary_stats(Hist),
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
    Cards = [1, 100, 1000, 10000, 25000, 50000, 100000, 1000000],
    Mods  = [hyper_gb, hyper_array, hyper_bisect],

    random:seed(1, 2, 3),

    Insert = fun (Card, EmptyFilter) ->
                     Values = generate_unique(Card),

                     Parent = self(),
                     spawn(fun () ->
                                   Start = os:timestamp(),
                                   H = insert_many(Values, EmptyFilter),
                                   Parent ! {h, H,
                                             timer:now_diff(os:timestamp(), Start)}
                           end),
                     receive {h, Hyper, ElapsedUs} ->
                             {Hyper, ElapsedUs}
                     end
             end,

    Union = fun (Card, EmptyFilter) ->
                    Left = insert_many(generate_unique(Card), EmptyFilter),
                    Right = insert_many(generate_unique(Card), EmptyFilter),

                    Parent = self(),
                    spawn(fun () ->
                                  Start = os:timestamp(),
                                  _Union = union(Left, Right),
                                  Parent ! {union, timer:now_diff(os:timestamp(), Start)}
                          end),
                    receive {union, ElapsedUs} -> ElapsedUs end
            end,

    R = [begin
             {H, ElapsedUs} = Insert(Card, new(P, Mod)),
             UnionUs = Union(Card, new(P, Mod)),

             {Mod, Registers} = H#hyper.registers,
             Fill = Mod:fold(fun (_, V, Acc) when V > 0 -> Acc+1;
                                 (_, _, Acc) -> Acc
                             end, 0, Registers),
             Bytes = case Mod of
                         hyper_bisect ->
                             case Registers of
                                 {sparse, R, _, _} ->
                                     bisect:size(R);
                                 {dense, R} ->
                                     byte_size(R)
                             end;
                         _ ->
                             erts_debug:flat_size(Registers) * 8
                     end,

             {Mod, P, Card, Fill, Bytes, ElapsedUs, UnionUs}
         end || Mod  <- Mods,
                P    <- Ps,
                Card <- Cards],

    io:format("~s ~s ~s ~s ~s ~s ~s~n",
              [string:left("module"    , 12, $ ),
               string:left("precision" , 10, $ ),
               string:left("card"      , 10, $ ),
               string:left("fill"      , 10, $ ),
               string:left("bytes"     , 10, $ ),
               string:left("insert us" , 10, $ ),
               string:left("union us"  , 10, $ )
              ]),

    lists:foreach(fun ({Mod, P, Card, Fill, Bytes, ElapsedUs, UnionUs}) ->
                          M = trunc(math:pow(2, P)),
                          Filled = lists:flatten(io_lib:format("~.2f", [Fill / M])),
                          AvgInsertUs = lists:flatten(io_lib:format("~.2f",
                                                                    [ElapsedUs / Card])),
                          UnionMs = lists:flatten(io_lib:format("~.2f", [UnionUs / 1000])),
                          io:format("~s ~s ~s ~s ~s ~s ~s~n",
                                    [
                                     string:left(atom_to_list(Mod)     , 12, $ ),
                                     string:left(integer_to_list(P)    , 10, $ ),
                                     string:left(integer_to_list(Card) , 10, $ ),
                                     string:left(Filled                , 10, $ ),
                                     string:left(integer_to_list(Bytes), 10, $ ),
                                     string:left(AvgInsertUs           , 10, $ ),
                                     string:left(UnionMs               , 10, $ )
                                    ])
                  end, R).
