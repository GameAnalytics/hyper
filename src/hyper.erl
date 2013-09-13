%% @doc: Implementation of HyperLogLog with bias correction as
%% described in the Google paper,
%% http://static.googleusercontent.com/external_content/untrusted_dlcp/
%% research.google.com/en//pubs/archive/40671.pdf
-module(hyper).
-include_lib("eunit/include/eunit.hrl").

-export([new/1, insert/2, card/1, union/1, union/2]).
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
    _M = trunc(pow(2, P)),
    Registers = bisect:new(2, 1),
    #hyper{p = P, registers = Registers}.


-spec insert(value(), filter()) -> filter().
insert(Value, #hyper{registers = Registers, p = P} = Hyper)
  when is_binary(Value) ->
    Hash = crypto:hash(sha, Value),
    <<Index:P, RegisterValue:P/bitstring, _/bitstring>> = Hash,

    ZeroCount = run_of_zeroes(RegisterValue) + 1,

    OldCount = case bisect:find(Registers, <<Index:16/integer>>) of
                   not_found -> 0;
                   <<V:8/integer>> -> V
               end,

    case OldCount < ZeroCount of
        true ->
            Hyper#hyper{registers = bisect:insert(Registers,
                                                  <<Index:16/integer>>,
                                                  <<ZeroCount:8/integer>>)};
        false ->
            Hyper
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
      #hyper{registers = RightRegisters} = Right) when
      Left#hyper.p =:= Right#hyper.p ->

    Start = now(),
    NewRegisters = bisect:merge(fun (Index, LeftValue, RightValue) ->
                                        max(LeftValue, RightValue)
                                end, LeftRegisters, RightRegisters),

    error_logger:info_msg("single union in ~.2f ms~n",
                          [timer:now_diff(now(), Start) / 1000]),

    Left#hyper{registers = NewRegisters}.


-spec card(filter()) -> float().
card(#hyper{registers = Registers, p = P}) ->
    M = trunc(pow(2, P)),
    RegisterSum = lists:sum(bisect_map(fun (_, Register) ->
                                              pow(2, -Register)
                                      end, Registers)),

    E = alpha(M) * pow(M, 2) / RegisterSum,
    Ep = case E =< 5 * M of
             true -> E - estimate_bias(E, P);
             false -> E
         end,

    V = length(lists:filter(fun (Register) -> Register =:= 0 end,
                            bisect_map(fun (_, R) -> R end,
                                       Registers))),
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
    {[
      {<<"p">>, Hyper#hyper.p},
      {<<"registers">>, encode_registers(Hyper#hyper.registers)}
     ]}.

-spec from_json(any()) -> filter().
from_json({Struct}) ->
    P = proplists:get_value(<<"p">>, Struct),
    M = trunc(math:pow(2, P)),
    Registers = array:fix(
                  array:resize(
                    M, array:from_list(
                         decode_registers(proplists:get_value(<<"registers">>, Struct)),
                         0))),

    #hyper{p = P, registers = Registers}.

encode_registers(Registers) ->
    ByteEncoded = array:foldl(fun (_I, V, A) ->
                                      [<<V:8/integer>> | A]
                              end, [], array:resize(Registers)),

    base64:encode(
      zlib:gzip(
        iolist_to_binary(
          lists:reverse(ByteEncoded)))).

decode_registers(B) ->
    decode_registers(zlib:gunzip(base64:decode(B)), []).

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

bisect_map(F, B) ->
    {K, V} = bisect:first(B),
    bisect_map(F, K, B, [F(K, V)]).

bisect_map(F, K, B, Acc) ->
    case bisect:next(B, K) of
        not_found ->
            lists:reverse(Acc);
        {NextK, NextV} ->
            bisect_map(F, NextK, B, [F(NextK, NextV) | Acc])
    end.




%%
%% TESTS
%%

%% basic_test() ->
%%     ?assertEqual(1, trunc(card(insert(<<"1">>, new(4))))).


%% serialization_test() ->
%%     Hyper = insert_many(generate_unique(1024), new(14)),
%%     ?assertEqual(trunc(card(Hyper)), trunc(card(from_json(to_json(Hyper))))).

%% encoding_test() ->
%%     Hyper = insert_many(generate_unique(100000), new(14)),
%%     ?assertEqual(trunc(card(Hyper)), trunc(card(from_json(to_json(Hyper))))).


%% error_range_test() ->
%%     Run = fun (Cardinality, P) ->
%%                   lists:foldl(fun (V, H) ->
%%                                       insert(V, H)
%%                               end, new(P), generate_unique(Cardinality))
%%           end,

%%     Report = lists:map(
%%                fun (Card) ->
%%                        CardString = string:left(integer_to_list(Card), 10, $ ),

%%                        Estimate = trunc(card(Run(Card, 14))),
%%                        io_lib:format("~s ~p~n", [CardString, Estimate])
%%                end, lists:seq(0, 50000, 1000)),
%%     error_logger:info_msg("~s~n", [Report]).


%% many_union_test() ->
%%     random:seed(1, 2, 3),
%%     Card = 100,
%%     NumSets = 3,

%%     Sets = [sets:from_list(generate_unique(Card)) || _ <- lists:seq(1, NumSets)],
%%     Filters = lists:map(fun (S) -> insert_many(sets:to_list(S), new(14)) end,
%%                         Sets),

%%     ?assert(abs(sets:size(sets:union(Sets)) - card(union(Filters)))
%%             < (Card * NumSets) * 0.1).


time_test_() ->
    {timeout, 30,
     fun() ->
             random:seed(1, 2, 3),
             Filters = [insert_many(generate_unique(100000), new(15)) || _ <- lists:seq(1, 31)],

             Start = now(),
             union(Filters),
             error_logger:info_msg("union of 31 filters in ~.2f ms~n",
                                   [timer:now_diff(now(), Start) / 1000]),
             ok
     end}.


%% union_test() ->
%%     random:seed(1, 2, 3),

%%     LeftDistinct = sets:from_list(generate_unique(10000)),

%%     RightDistinct = sets:from_list(generate_unique(5000)
%%                                    ++ lists:sublist(sets:to_list(LeftDistinct),
%%                                                     5000)),

%%     LeftHyper = insert_many(sets:to_list(LeftDistinct), new(13)),
%%     RightHyper = insert_many(sets:to_list(RightDistinct), new(13)),

%%     UnionHyper = union(LeftHyper, RightHyper),
%%     Intersection = card(LeftHyper) + card(RightHyper) - card(UnionHyper),

%%     error_logger:info_msg("left distinct: ~p~n"
%%                           "right distinct: ~p~n"
%%                           "true union: ~p~n"
%%                           "true intersection: ~p~n"
%%                           "estimated union: ~p~n"
%%                           "estimated intersection: ~p~n",
%%                           [sets:size(LeftDistinct),
%%                            sets:size(RightDistinct),
%%                            sets:size(
%%                              sets:union(LeftDistinct, RightDistinct)),
%%                            sets:size(
%%                              sets:intersection(LeftDistinct, RightDistinct)),
%%                            card(UnionHyper),
%%                            Intersection
%%                           ]).

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
                        
