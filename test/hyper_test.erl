-module(hyper_test).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(hyper, {p, registers}). % copy of #hyper in hyper.erl

hyper_test_() ->
    {foreach, fun () -> ok end, fun (_) -> ok end,
     [
      ?_test(basic_t()),
      ?_test(serialization_t()),
      {timeout, 60, ?_test(backend_t())},
      ?_test(encoding_t()),
      ?_test(register_sum_t()),
      {timeout, 30, ?_test(error_range_t())},
      ?_test(many_union_t()),
      ?_test(union_t()),
      ?_test(small_big_union_t()),
      ?_test(intersect_card_t()),
      {timeout, 600, fun () -> ?assert(proper:quickcheck(prop_set(),
                                                         [{max_size, 1000},
                                                          {numtests, 1000},
                                                          {to_file, user}])) end},
      {timeout, 600, fun () -> ?assert(proper:quickcheck(prop_serialize(),
                                                         [{max_size, 1000},
                                                          {numtests, 1000},
                                                          {to_file, user}])) end},
      ?_test(bad_serialization_t())
     ]}.

basic_t() ->
    [?assertEqual(1, trunc(
                       hyper:card(
                         hyper:insert(<<"1">>, hyper:new(4, Mod)))))
     || Mod <- backends()].


serialization_t() ->
    Mod = hyper_binary_rle,
    Hyper = hyper:compact(hyper:insert_many(generate_unique(10), hyper:new(5, Mod))),

    {Mod, L} = Hyper#hyper.registers,
    {Mod, R} = (hyper:compact(
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
    Values = generate_unique(10000),
    P = 9,
    M = trunc(math:pow(2, P)),

    Gb     = hyper:compact(hyper:insert_many(Values, hyper:new(P, hyper_gb))),
    Array  = hyper:compact(hyper:insert_many(Values, hyper:new(P, hyper_array))),
    Bisect = hyper:compact(hyper:insert_many(Values, hyper:new(P, hyper_bisect))),
    Binary = hyper:compact(hyper:insert_many(Values, hyper:new(P, hyper_binary))),
    Rle    = hyper:compact(hyper:insert_many(Values, hyper:new(P, hyper_binary_rle))),

    {hyper_gb    , GbRegisters}     = Gb#hyper.registers,
    {hyper_array , ArrayRegisters}  = Array#hyper.registers,
    {hyper_bisect, BisectRegisters} = Bisect#hyper.registers,
    {hyper_binary, BinaryRegisters} = Binary#hyper.registers,
    {hyper_binary_rle, RleRegisters} = Rle#hyper.registers,

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

                                  case dict:find(Index, Registers) of
                                      {ok, R} when R > ZeroCount ->
                                          Registers;
                                      _ ->
                                          dict:store(Index, ZeroCount, Registers)
                                  end
                          end, dict:new(), Values),
    ExpectedBytes = iolist_to_binary(
                      [begin
                           case dict:find(I, ExpectedRegisters) of
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
    ?assertEqual(ExpectedBytes, hyper_binary_rle:encode_registers(RleRegisters)),

    %% error_logger:info_msg("~p~n~p~n", [hyper:from_json(
    %%                                      hyper:to_json(Array),
    %%                                      hyper_binary_rle),
    %%                                   Rle]),

    ?assertEqual(hyper:card(Gb),
                 hyper:card(hyper:from_json(hyper:to_json(Array), hyper_gb))),
    ?assertEqual(Array, hyper:from_json(hyper:to_json(Array), hyper_array)),
    ?assertEqual(Array, hyper:from_json(hyper:to_json(Bisect), hyper_array)),
    ?assertEqual(Bisect, hyper:from_json(hyper:to_json(Array), hyper_bisect)),
    ?assertEqual(Rle, hyper:from_json(hyper:to_json(Array), hyper_binary_rle)),


    ?assertEqual(hyper:to_json(Gb), hyper:to_json(Array)),
    ?assertEqual(hyper:to_json(Gb), hyper:to_json(Bisect)),
    ?assertEqual(hyper:to_json(Gb), hyper:to_json(Binary)),
    ?assertEqual(hyper:to_json(Gb), hyper:to_json(Rle)),

    ?assertEqual(hyper:card(Gb), hyper:card(Array)),
    ?assertEqual(hyper:card(Gb), hyper:card(Bisect)),
    ?assertEqual(hyper:card(Gb), hyper:card(Binary)),
    ?assertEqual(hyper:card(Gb), hyper:card(Rle)).




encoding_t() ->
    [begin
         P = 15,
         M = trunc(math:pow(2, P)),
         Hyper = hyper:insert_many(generate_unique(1000), hyper:new(P, Mod)),
         ?assertEqual(trunc(hyper:card(Hyper)),
                      trunc(hyper:card(hyper:from_json(hyper:to_json(Hyper), Mod)))),

         {Struct} = hyper:to_json(Hyper),
         Serialized = zlib:gunzip(
                        base64:decode(
                          proplists:get_value(<<"registers">>, Struct))),

         WithPadding =  <<Serialized/binary, 0>>,

         B            = Mod:encode_registers(Mod:decode_registers(Serialized, P)),
         BWithPadding = Mod:encode_registers(Mod:decode_registers(WithPadding, P)),
         ?assertEqual(M, byte_size(B)),
         ?assertEqual(M, byte_size(BWithPadding)),
         ?assertEqual(B, BWithPadding)
     end || Mod <- backends()].


register_sum_t() ->
    Mods = backends(),
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
         Compact = Mod:compact(Registers),
         ?assertEqual({Mod, ExpectedSum}, {Mod, Mod:register_sum(Compact)})
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
    Card = 100,
    NumSets = 3,

    [begin
         M = trunc(math:pow(2, P)),
         Error = 1.04 / math:sqrt(M),

         Sets = [sets:from_list(generate_unique(Card))
                 || _ <- lists:seq(1, NumSets)],
         Filters = [hyper:insert_many(sets:to_list(S), hyper:new(P, Mod))
                    || S <- Sets],

         ExpectedFilter = hyper:compact(
                            hyper:insert_many(
                              lists:flatten([sets:to_list(S) || S <- Sets]),
                              hyper:new(P, Mod))),
         H = hyper:union(Filters),

         {Mod, ExpectedRegisters} = ExpectedFilter#hyper.registers,
         {Mod, ActualRegisters} = H#hyper.registers,

         ?assertEqual(Mod:encode_registers(ExpectedRegisters),
                      Mod:encode_registers(ActualRegisters)),

         Delta = abs(sets:size(sets:union(Sets)) - hyper:card(hyper:union(Filters))),
         case Delta < (Card * NumSets * Error) of
             true ->
                 ok;
             false ->
                 error_logger:info_msg("too high error, expected ~.2f%, actual ~.2f%~n"
                                       "~p, p = ~p, card = ~p",
                                       [Error, Delta / (Card * NumSets), Mod, P, Card]),
                 ?assert(false)
         end
     end || Mod <- [hyper_binary_rle],
            P <- [15]].



union_t() ->
    random:seed(1, 2, 3),
    Mod = hyper_binary_rle,

    LeftDistinct = sets:from_list(generate_unique(100)),

    RightDistinct = sets:from_list(generate_unique(50)
                                   ++ lists:sublist(sets:to_list(LeftDistinct),
                                                    50)),

    LeftHyper = hyper:insert_many(sets:to_list(LeftDistinct),
                                  hyper:new(13, Mod)),
    RightHyper = hyper:insert_many(sets:to_list(RightDistinct),
                                   hyper:new(13, Mod)),

    UnionHyper = hyper:union([LeftHyper, RightHyper]),
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



bad_serialization_t() ->
    [begin
         P = 15,
         M = trunc(math:pow(2, P)),
         {ok, WithNewlines} = file:read_file("../test/filter.txt"),
         Raw = case zlib:gunzip(
                      base64:decode(
                        binary:replace(WithNewlines, <<"\n">>, <<>>))) of
                   <<RawBytes:M/binary>> ->
                       RawBytes;
                   <<RawBytes:M/binary, 0>> ->
                       %% TODO: test padding
                       RawBytes
               end,

         H = hyper:from_json({[{<<"p">>, 15},
                               {<<"registers">>, base64:encode(zlib:gzip(Raw))}]},
                             Mod),
         {Mod, Registers} = H#hyper.registers,
         Encoded = Mod:encode_registers(Registers),

         ?assertEqual(size(Raw), size(Encoded)),
         lists:foreach(fun (I) ->
                               ?assertEqual(binary:at(Raw, I),
                                            binary:at(Encoded, I))
                       end, lists:seq(0, size(Encoded) - 1)),

         ?assertEqual(Raw, Mod:encode_registers(Registers)),

         ?assertEqual({[{<<"p">>, 15},
                        {<<"registers">>, base64:encode(zlib:gzip(Raw))}]},
                      hyper:to_json(H))
     end || Mod <- backends()].

%%
%% PROPERTIES
%%

backends() ->
    [hyper_gb, hyper_array, hyper_bisect, hyper_binary, hyper_binary_rle].


gen_values() ->
    ?SIZED(Size, gen_values(Size)).

gen_values(0) ->
    [<<(random:uniform(100000000000000)):64/integer>>];
gen_values(Size) ->
    [<<(random:uniform(100000000000000)):64/integer>> | gen_values(Size-1)].

gen_getset(P) ->
    ?SIZED(Size, gen_getset(Size, P)).

gen_getset(0, _P) ->
    [];
gen_getset(Size, P) ->
    M = trunc(math:pow(2, P)),
    ?LET({I, V}, {choose(0, M-1), choose(1, 6)},
         [{I, V} | gen_getset(Size-1, P)]).


prop_set() ->
    ?FORALL(
       {Mod, P}, {oneof(backends()), choose(4, 16)},
       ?FORALL(
          Values, gen_getset(P),
          begin
              R = lists:foldl(
                    fun ({Index, ZeroCount}, Register) ->
                            Mod:set(Index, ZeroCount, Register)
                    end, Mod:new(P), Values),
              Max = lists:foldl(fun ({I, V}, Acc) ->
                                        case dict:find(I, Acc) of
                                            {ok, OtherV} when OtherV >= V ->
                                                Acc;
                                            _ ->
                                                dict:store(I, V, Acc)
                                        end
                                end, dict:new(), Values),
              Expected = lists:map(fun (I) ->
                                           case dict:find(I, Max) of
                                               {ok, V} ->
                                                   <<V:8/integer>>;
                                               error ->
                                                   <<0>>
                                           end
                                   end, lists:seq(0, trunc(math:pow(2, P)) - 1)),

              case Mod:encode_registers(Mod:compact(R))
                  =:= iolist_to_binary(Expected) of
                  true ->
                      true;
                  false ->
                      %% error_logger:info_msg("values~n~p~n"
                      %%                       "encoded~n~p~n"
                      %%                       "expected~n~p~n",
                      %%                       [Values,
                      %%                        Mod:encode_registers(R),
                      %%                        iolist_to_binary(Expected)]),
                        false
              end
          end)).

prop_serialize() ->
    ?FORALL(
       {Mod, P}, {oneof(backends()), choose(4, 16)},
       ?FORALL(
          Values, gen_values(),
          begin
              H = hyper:compact(hyper:insert_many(Values, hyper:new(P, Mod))),
              H =:= hyper:compact(
                      hyper:from_json(
                        hyper:to_json(H), Mod))
          end)).

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
