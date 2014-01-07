%% @doc: Implementation of HyperLogLog with bias correction as
%% described in the Google paper,
%% http://static.googleusercontent.com/external_content/untrusted_dlcp/
%% research.google.com/en//pubs/archive/40671.pdf
-module(hyper).
-include_lib("eunit/include/eunit.hrl").

-export([new/1, insert/2, card/1, union/1, union/2, intersect_card/2]).
-export([to_json/1, from_json/1]).
-compile(native).

-record(hyper, {p, registers}).

-type precision() :: 4..16.
-type value()     :: binary().
-type filter()    :: #hyper{}.

-export_type([filter/0]).


%%
%% API
%%

-spec new(precision()) -> filter().
new(P) when 4 =< P andalso P =< 16 ->
    #hyper{p = P, registers = gb_trees:empty()}.


-spec insert(value(), filter()) -> filter().
insert(Value, #hyper{registers = Registers, p = P} = Hyper)
  when is_binary(Value) ->
    Hash = crypto:hash(sha, Value),
    <<Index:P, RegisterValue:P/bitstring, _/bitstring>> = Hash,

    ZeroCount = run_of_zeroes(RegisterValue) + 1,

    case gb_trees:lookup(Index, Hyper#hyper.registers) of
        {value, Small} when Small < ZeroCount ->
            Hyper#hyper{registers = gb_trees:enter(Index, ZeroCount, Registers)};
        {value, Large} when ZeroCount =< Large ->
            Hyper;
        none ->
            Hyper#hyper{registers = gb_trees:enter(Index, ZeroCount, Registers)}
    end;

insert(_Value, _Hyper) ->
    error(badarg).


-spec union([filter()]) -> filter().
union(Filters) when is_list(Filters) ->
    %% Must have the same P
    case lists:usort(lists:map(fun (H) -> H#hyper.p end, Filters)) of
        [P] ->
            lists:foldl(fun union/2, hyper:new(P), Filters)
    end.

union(#hyper{registers = LeftRegisters} = Left,
      #hyper{registers = RightRegisters} = Right)
  when Left#hyper.p =:= Right#hyper.p ->
    %% NewRegisters =
    %%     gb_trees:from_orddict(
    %%       orddict:merge(fun (_Index, L, R) -> max(L, R) end,
    %%                     gb_trees:to_list(LeftRegisters),
    %%                     gb_trees:to_list(RightRegisters))),

    NewRegisters = gb_fold(fun (Index, L, Registers) ->
                                   case gb_trees:lookup(Index, Registers) of
                                       {value, R} when R < L ->
                                           gb_trees:enter(Index, L, Registers);
                                       {value, _} ->
                                           Registers;
                                       none ->
                                           gb_trees:enter(Index, L, Registers)
                                   end
                           end, RightRegisters, LeftRegisters),

    Right#hyper{registers = NewRegisters}.


gb_fold(F, A, {_, T}) when is_function(F, 3) ->
    gb_fold_1(F, A, T).

gb_fold_1(F, Acc0, {Key, Value, Small, Big}) ->
    Acc1 = gb_fold_1(F, Acc0, Small),
    Acc = F(Key, Value, Acc1),
    gb_fold_1(F, Acc, Big);
gb_fold_1(_, Acc, _) ->
    Acc.


%% NOTE: use with caution, no guarantees on accuracy.
-spec intersect_card(filter(), filter()) -> float().
intersect_card(Left, Right) when Left#hyper.p =:= Right#hyper.p ->
    max(0.0, (card(Left) + card(Right)) - card(union(Left, Right))).


-spec card(filter()) -> float().
card(#hyper{registers = Registers, p = P}) ->
    M = trunc(pow(2, P)),

    RegisterSum = register_sum(M-1, Registers, 0),

    E = alpha(M) * pow(M, 2) / RegisterSum,
    Ep = case E =< 5 * M of
             true -> E - estimate_bias(E, P);
             false -> E
         end,

    V = count_zeros(M-1, Registers, 0),

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
from_json({Struct}) ->
    P = proplists:get_value(<<"p">>, Struct),
    {_, Registers} = lists:foldl(fun (0, {I, A}) ->
                                         {I+1, A};
                                     (V, {I, A}) ->
                                         {I+1, gb_trees:enter(I, V, A)}
                                 end,
                                 {0, gb_trees:empty()},
                                 decode_registers(
                                   proplists:get_value(<<"registers">>, Struct))),

    #hyper{p = P, registers = Registers}.


encode_registers(M, Registers) ->
    %% FIXME: shouldn't this be M-1 rather than M?
    ByteEncoded = encode_registers(M, Registers, []),
    base64:encode(
      zlib:gzip(
        iolist_to_binary(ByteEncoded))).

encode_registers(I, _Registers, ByteEncoded) when I < 0 ->
    ByteEncoded;

encode_registers(I, Registers, ByteEncoded) when I >= 0 ->
    Byte = case gb_trees:lookup(I, Registers) of
               {value, V} ->
                   <<V:8/integer>>;
               none ->
                   <<0>>
           end,
    encode_registers(I - 1, Registers, [Byte | ByteEncoded]).


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


register_sum(I, _Registers, Sum) when I < 0 ->
    Sum;
register_sum(I, Registers, Sum) when I >= 0 ->
    Val = case gb_trees:lookup(I, Registers) of
              {value, R} ->
                  pow(2, -R);
              none ->
                  pow(2, -0)
          end,
    register_sum(I - 1, Registers, Sum + Val).

count_zeros(I, _Registers, Count) when I < 0 ->
    Count;
count_zeros(I, Registers, Count) when I >= 0 ->
    %% FIXME: shouldn't we also count the value if it is equal to 0?
    Val = case gb_trees:lookup(I, Registers) =:= none of
              true  -> 1;
              false -> 0
          end,
    count_zeros(I - 1, Registers, Count + Val).



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
    ?assertEqual(1, trunc(card(insert(<<"1">>, new(4))))).


serialization_test() ->
    Hyper = insert_many(generate_unique(10), new(14)),

    L = Hyper#hyper.registers,
    R = (from_json(to_json(Hyper)))#hyper.registers,

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
                  end, lists:seq(1000, 50000, 1000)).



many_union_test() ->
    random:seed(1, 2, 3),
    Card = 100,
    NumSets = 3,

    Sets = [sets:from_list(generate_unique(Card)) || _ <- lists:seq(1, NumSets)],
    Filters = lists:map(fun (S) -> insert_many(sets:to_list(S), new(14)) end,
                        Sets),
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
