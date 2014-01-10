-module(hyper_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(hyper, {p, registers}). % copy of #hyper in hyper.erl

hyper_test_() ->
    {foreach, fun () -> ok end, fun (_) -> ok end,
     [
      ?_test(basic_t()),
      ?_test(serialization_t()),
      {timeout, 30, ?_test(backend_t())},
      ?_test(encoding_t()),
      ?_test(register_sum_t()),
      {timeout, 30, ?_test(error_range_t())},
      ?_test(many_union_t()),
      ?_test(union_t()),
      ?_test(small_big_union_t()),
      ?_test(intersect_card_t()),
      {timeout, 600, fun () -> [] = proper:module(?MODULE) end}
     ]}.

basic_t() ->
    [?assertEqual(1, trunc(
                       hyper:card(
                         hyper:insert(<<"1">>, hyper:new(4, Mod)))))
     || Mod <- [hyper_bisect, hyper_binary, hyper_gb, hyper_array]].


serialization_t() ->
    Mod = hyper_binary,
    Hyper = hyper:compact(hyper:insert_many(generate_unique(10), hyper:new(5, Mod))),

    {hyper_binary, L} = Hyper#hyper.registers,
    {hyper_binary, R} = (hyper:compact(
                           hyper:from_json(
                             hyper:to_json(Hyper), Mod)))#hyper.registers,

    ?assertEqual(trunc(hyper:card(Hyper)),
                 trunc(
                   hyper:card(
                     hyper:from_json(
                       hyper:to_json(Hyper), Mod)))),
    ?assertEqual(Hyper#hyper.p, (hyper:from_json(
                                   hyper:to_json(Hyper), Mod))#hyper.p),
    ?assertEqual(L, R).


backend_t() ->
    Values = generate_unique(1000000),
    P = 9,
    M = trunc(math:pow(2, P)),

    Gb     = hyper:compact(hyper:insert_many(Values, hyper:new(P, hyper_gb))),
    Array  = hyper:compact(hyper:insert_many(Values, hyper:new(P, hyper_array))),
    Bisect = hyper:compact(hyper:insert_many(Values, hyper:new(P, hyper_bisect))),
    Binary = hyper:compact(hyper:insert_many(Values, hyper:new(P, hyper_binary))),


    {hyper_gb    , GbRegisters}     = Gb#hyper.registers,
    {hyper_array , ArrayRegisters}  = Array#hyper.registers,
    {hyper_bisect, BisectRegisters} = Bisect#hyper.registers,
    {hyper_binary, BinaryRegisters} = Binary#hyper.registers,

    %% error_logger:info_msg("Gb:     ~p~nArray:  ~p~nBisect: ~p~nBinary: ~p~n",
    %%                       [hyper_gb:encode_registers(GbRegisters),
    %%                        hyper_array:encode_registers(ArrayRegisters),
    %%                        hyper_bisect:encode_registers(BisectRegisters),
    %%                        hyper_binary:encode_registers(BinaryRegisters)]),


    ExpectedRegisters = lists:foldl(
                          fun (Value, Registers) ->
                                  Hash = crypto:hash(sha, Value),
                                  <<Index:P, RegisterValue:P/bitstring,
                                    _/bitstring>> = Hash,
                                  ZeroCount = hyper:run_of_zeroes(RegisterValue)
                                      + 1,

                                  case orddict:find(Index, Registers) of
                                      {ok, R} when R > ZeroCount ->
                                          Registers;
                                      _ ->
                                          orddict:store(Index, ZeroCount, Registers)
                                  end
                          end, orddict:new(), Values),
    ExpectedBytes = iolist_to_binary(
                      [begin
                           case orddict:find(I, ExpectedRegisters) of
                               {ok, V} ->
                                   <<V:8/integer>>;
                               error ->
                                   <<0>>
                           end
                       end || I <- lists:seq(0, M-1)]),

    ?assertEqual(ExpectedBytes, hyper_gb:encode_registers(GbRegisters)),
    ?assertEqual(ExpectedBytes, hyper_array:encode_registers(ArrayRegisters)),
    ?assertEqual(ExpectedBytes, hyper_bisect:encode_registers(BisectRegisters)),
    ?assertEqual(ExpectedBytes, hyper_binary:encode_registers(BinaryRegisters)),

    ?assertEqual(hyper:card(Gb),
                 hyper:card(hyper:from_json(hyper:to_json(Array), hyper_gb))),
    ?assertEqual(Array, hyper:from_json(hyper:to_json(Array), hyper_array)),
    ?assertEqual(Array, hyper:from_json(hyper:to_json(Bisect), hyper_array)),
    ?assertEqual(Bisect, hyper:from_json(hyper:to_json(Array), hyper_bisect)),

    ?assertEqual(hyper:to_json(Gb), hyper:to_json(Array)),
    ?assertEqual(hyper:to_json(Gb), hyper:to_json(Bisect)),
    ?assertEqual(hyper:to_json(Gb), hyper:to_json(Binary)),

    ?assertEqual(hyper:card(Gb), hyper:card(Array)),
    ?assertEqual(hyper:card(Gb), hyper:card(Bisect)),
    ?assertEqual(hyper:card(Gb), hyper:card(Binary)).




encoding_t() ->
    Hyper = hyper:insert_many(generate_unique(10), hyper:new(4)),
    ?assertEqual(trunc(hyper:card(Hyper)),
                 trunc(hyper:card(hyper:from_json(hyper:to_json(Hyper))))).


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
                                      hyper:insert(V, H)
                              end, hyper:new(P, Mod), generate_unique(Cardinality))
          end,
    ExpectedError = 0.02,
    P = 14,
    random:seed(1, 2, 3),

    [begin
         Estimate = trunc(hyper:card(Run(Card, P, Mod))),
         ?assert(abs(Estimate - Card) < Card * ExpectedError)
     end || Card <- lists:seq(1000, 50000, 5000),
            Mod <- Mods].

many_union_t() ->
    random:seed(1, 2, 3),
    Card = 1000,
    NumSets = 3,

    Sets = [sets:from_list(generate_unique(Card)) || _ <- lists:seq(1, NumSets)],
    Filters = lists:map(fun (S) ->
                                hyper:insert_many(sets:to_list(S),
                                                  hyper:new(10, hyper_bisect))
                        end, Sets),

    ?assert(abs(sets:size(sets:union(Sets)) - hyper:card(hyper:union(Filters)))
            < (Card * NumSets) * 0.1).



union_t() ->
    random:seed(1, 2, 3),

    LeftDistinct = sets:from_list(generate_unique(10000)),

    RightDistinct = sets:from_list(generate_unique(5000)
                                   ++ lists:sublist(sets:to_list(LeftDistinct),
                                                    5000)),

    LeftHyper = hyper:insert_many(sets:to_list(LeftDistinct), hyper:new(13)),
    RightHyper = hyper:insert_many(sets:to_list(RightDistinct), hyper:new(13)),

    UnionHyper = hyper:union(LeftHyper, RightHyper),
    Intersection = hyper:card(LeftHyper)
        + hyper:card(RightHyper) - hyper:card(UnionHyper),

    ?assert(abs(hyper:card(UnionHyper) -
                    sets:size(sets:union(LeftDistinct, RightDistinct)))
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

    SmallHyper = hyper:insert_many(sets:to_list(SmallSet),
                                   hyper:new(15, hyper_bisect)),
    BigHyper   = hyper:insert_many(sets:to_list(BigSet),
                                   hyper:new(15, hyper_bisect)),
    ?assertMatch({hyper_bisect, {sparse, _, _, _}}, SmallHyper#hyper.registers),
    ?assertMatch({hyper_bisect, {dense, _}}, BigHyper#hyper.registers),

    UnionHyper = hyper:union(SmallHyper, BigHyper),
    TrueUnion = sets:size(sets:union(SmallSet, BigSet)),
    ?assert(abs(hyper:card(UnionHyper) - TrueUnion) < TrueUnion * 0.01).



intersect_card_t() ->
    random:seed(1, 2, 3),

    LeftDistinct = sets:from_list(generate_unique(10000)),

    RightDistinct = sets:from_list(generate_unique(5000)
                                   ++ lists:sublist(sets:to_list(LeftDistinct),
                                                    5000)),

    LeftHyper = hyper:insert_many(sets:to_list(LeftDistinct), hyper:new(13)),
    RightHyper = hyper:insert_many(sets:to_list(RightDistinct), hyper:new(13)),

    IntersectCard = hyper:intersect_card(LeftHyper, RightHyper),

    ?assert(IntersectCard =< hyper:card(hyper:union(LeftHyper, RightHyper))),

    %% NOTE: we can't really say much about the error here,
    %% so just pick something and see if the intersection makes sense
    Error = 0.05,
    ?assert((abs(5000 - IntersectCard) / 5000) =< Error).


%%
%% PROPERTIES
%%

backends() ->
    [hyper_gb, hyper_array, hyper_bisect, hyper_binary].

gen_values() ->
    %% ?LET(S, oneof(lists:seq(1, 100000, 10)), generate_unique(S)).
    ?SIZED(Size, generate_unique(Size*4)).

expected_bytes(P, Values) ->
    M = trunc(math:pow(2, P)),
    ExpectedRegisters = lists:foldl(
                          fun (Value, Registers) ->
                                  Hash = crypto:hash(sha, Value),
                                  <<Index:P, RegisterValue:P/bitstring,
                                    _/bitstring>> = Hash,
                                  ZeroCount = hyper:run_of_zeroes(RegisterValue)
                                      + 1,

                                  case orddict:find(Index, Registers) of
                                      {ok, R} when R > ZeroCount ->
                                          Registers;
                                      _ ->
                                          orddict:store(Index, ZeroCount, Registers)
                                  end
                          end, orddict:new(), Values),
    iolist_to_binary(
      [begin
           case orddict:find(I, ExpectedRegisters) of
               {ok, V} ->
                   <<V:8/integer>>;
               error ->
                   <<0>>
           end
       end || I <- lists:seq(0, M-1)]).


prop_getset() ->
    ?FORALL({Mod, P, Values}, {oneof(backends()), range(4, 16), gen_values()},
            begin
                L = lists:map(
                      fun (Value) ->
                              Hash = crypto:hash(sha, Value),
                              <<Index:P, RegisterValue:P/bitstring,
                                _/bitstring>> = Hash,
                              ZeroCount = hyper:run_of_zeroes(RegisterValue)
                                  + 1,
                              {Index, ZeroCount}
                      end, Values),

                R = lists:foldl(
                      fun ({Index, ZeroCount}, Register) ->
                              Mod:set(Index, ZeroCount, Register)
                      end, Mod:new(P), L),
                case Mod:encode_registers(Mod:compact(R))
                    =:= expected_bytes(P, Values) of
                    true ->
                        true;
                    false ->
                        error_logger:info_msg("values~n~pencoded~n~p~nexpected~n~p~n",
                                              [L,
                                               Mod:encode_registers(R),
                                               expected_bytes(P, Values)]),
                        false
                end
            end).


%%
%% HELPERS
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
