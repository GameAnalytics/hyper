-module(hyper).
-include_lib("eunit/include/eunit.hrl").

-record(hyper, {alpha, p, m, registers}).

new(P) when 4 =< P andalso P =< 16 ->
    M = trunc(math:pow(2, P)),
    Alpha = case M of
                16 ->
                    0.673;
                32 ->
                    0.697;
                64 ->
                    0.709;
                M ->
                    0.7213 / (1 + 1.079 / M)
            end,

    #hyper{alpha = Alpha, m = M, p = P,
           registers = array:new([{size, M}, {fixed, true}, {default, 0}])}.


insert(Value, #hyper{registers = Registers, p = P} = Hyper) ->
    Hash = erlang:phash2(Value, 4294967296), % 2^32
    <<Index:P, RegisterValue/bitstring>> = <<Hash:32>>,

    ZeroCount = run_of_zeroes(RegisterValue) + 1,

    case array:get(Index, Hyper#hyper.registers) < ZeroCount of
        true ->
            Hyper#hyper{registers = array:set(Index, ZeroCount, Registers)};
        false ->
            Hyper
    end.

pow(X, Y) ->
    math:pow(X, Y).

union(#hyper{registers = LeftRegisters} = Left,
      #hyper{registers = RightRegisters} = Right) when
      Left#hyper.m =:= Right#hyper.m ->


    NewRegisters = lists:zipwith(fun max/2,
                                 tuple_to_list(LeftRegisters),
                                 tuple_to_list(RightRegisters)),

    Left#hyper{registers = list_to_tuple(NewRegisters)}.

card(#hyper{alpha = Alpha, m = M, registers = Registers}) ->
    RegistersPow2 =
        lists:map(fun (Register) ->
                          pow(2, -Register)
                  end, array:to_list(Registers)),
    RegisterSum = lists:sum(RegistersPow2),

    DVEst = Alpha * pow(M, 2) * (1 / RegisterSum),

    TwoPower32 = pow(2, 32),

    if
        DVEst < 5/2 * M ->
            ZeroRegisters =
                length(
                  lists:filter(fun (Register) -> Register =:= 0 end,
                               array:to_list(Registers))),
            case ZeroRegisters of
                0 ->
                    DVEst;
                _ ->
                    M * math:log(M / ZeroRegisters)
            end;
        DVEst =< (1/30 * TwoPower32) ->
            DVEst;
        DVEst >= (1/30 * TwoPower32) ->
            pow(-2, 32) * math:log(1 - DVEst/TwoPower32)
    end.



%% @doc: Count run of zeroes from the right
%% run_of_zeroes(B) ->
%%     run_of_zeroes(B, bit_size(B)).

run_of_zeroes(B) ->
    run_of_zeroes(1, B).

run_of_zeroes(I, B) ->
    case B of
        <<0:I, _/bitstring>> ->
            run_of_zeroes(I + 1, B);
        _ ->
            I - 1
    end.


%%
%% TESTS
%%

basic_test() ->
    error_logger:info_msg("~p~n", [card(insert(1, new(4)))]).


ranges_test_() ->
    {timeout, 60000,
     fun() ->
             Card = 1000000,
             {GenerateUsec, Values} = timer:tc(fun () -> generate_unique(Card) end),
             error_logger:info_msg("generated ~p unique in ~.2f ms~n",
                                   [Card, GenerateUsec / 1000]),

             {Usec, Hyper} = timer:tc(
                               fun () ->
                                       lists:foldl(fun (V, H) ->
                                                           insert(V, H)
                                                   end,
                                                   new(16), Values)
                               end),
             error_logger:info_msg("true distinct: ~p, estimated: ~p, in ~.2f ms~n"
                                   "~.2f per second~n",
                                   [Card, card(Hyper), Usec / 1000,
                                    Card / (Usec / 1000 / 1000)])
     end}.



%% union_test() ->
%%     random:seed(1, 2, 3),
%%     LeftDistinct = sets:from_list(
%%                      [random:uniform(10000) || _ <- lists:seq(1, 10*1000)]),

%%     RightDistinct = sets:from_list(
%%                       [random:uniform(5000) || _ <- lists:seq(1, 10000)]),

%%     LeftHyper = add_many(sets:to_list(LeftDistinct),
%%                          new(16)),

%%     RightHyper = add_many(sets:to_list(RightDistinct),
%%                           new(16)),

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


report_wrapper_test_() ->
    [{timeout, 600000000, ?_test(estimate_report())}].

estimate_report() ->
    random:seed(erlang:now()),
    Ps = lists:seq(4, 16, 1),
    Cardinalities = [100, 1000, 10000, 100000, 1000000],
    Repetitions = 60,

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
    %% Rand = [random:uniform(100000000000000) || _ <- lists:seq(1, N)],
    %% sets:to_list(
    %%   generate_unique(
    %%     %% sets:from_list(Rand),
    %%     sets:from_list(random_bytes(N)),
    %%     N)).

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
                        
