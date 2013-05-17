-module(hyper).
-include_lib("eunit/include/eunit.hrl").

-record(hyper, {alpha, b, m, registers}).

new(B) when 4 =< B andalso B =< 16 ->
    M = trunc(math:pow(2, B)),
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

    #hyper{alpha = Alpha, m = M, b = B,
           registers = erlang:make_tuple(M, 0)}.


add(Value, #hyper{registers = Registers} = Hyper) ->
    Hash = erlang:phash2(Value, 4294967296), % 2^32
    %% error_logger:info_msg("hash: ~p~n", [Hash]),

    ValueSize = 32 - Hyper#hyper.b,
    B = Hyper#hyper.b,
    <<RegisterValue:ValueSize, Index:B>> = <<Hash:32>>,

    %% error_logger:info_msg("index: ~p, register value: ~p~n",
    %%                       [Index, RegisterValue]),

    %% error_logger:info_msg(integer_to_list(RegisterValue, 2) ++ "~n"),

    %% error_logger:info_msg("~p~n",
    %%                       [run_of_zeroes(RegisterValue)]),

    ZeroCount = run_of_zeroes(RegisterValue),

    case element(Index+1, Hyper#hyper.registers) < ZeroCount of
        true ->
            Hyper#hyper{registers = setelement(Index+1, Registers, ZeroCount)};
        false ->
            Hyper
    end.

pow(X, Y) ->
    math:pow(X, Y).

card(#hyper{alpha = Alpha, m = M, registers = Registers}) ->
    RegistersPow2 =
        lists:map(fun (Register) ->
                          pow(2, -Register)
                  end, tuple_to_list(Registers)),
    RegisterSum = lists:sum(RegistersPow2),

    DVEst = Alpha * pow(M, 2) * (1 / RegisterSum),

    TwoPower32 = trunc(math:pow(2, 32)),

    if
        DVEst < 5/2 * M ->
            ZeroRegisters =
                length(
                  lists:filter(fun (Register) -> Register =:= 0 end,
                               tuple_to_list(Registers))),
            case ZeroRegisters of
                0 ->
                    DVEst;
                _ ->
                    M * math:log(M / ZeroRegisters)
            end;
        DVEst =< (1/30 * TwoPower32) ->
            DVEst;
        DVEst >= (1/30 * TwoPower32) ->
            trunc(math:pow(-2, 32)) * math:log(1 - DVEst/TwoPower32)
    end.



%% @doc: Count run of zeroes from the right
%% run_of_zeroes(B) ->
%%     run_of_zeroes(B, bit_size(B)).

run_of_zeroes(B) ->
    1 + leading_zeroes(lists:reverse(integer_to_list(B, 2))).

leading_zeroes([$0 | Rest]) ->
    1 + leading_zeroes(Rest);
leading_zeroes([$1 | _]) ->
    0.



%% run_of_zeroes(B, I) ->
%%     Size = bit_size(B) - (bit_size(B) - I),
%%     Right = bit_size(B) - Size,
%%     case B of
%%         <<_:Size, 0:Right>> ->
%%             run_of_zeroes(B, I+1);
%%         << ->


%%
%% TESTS
%%

%% basic_test() ->
%%     error_logger:info_msg("~p~n", [card(add(1, new(4)))]).

ranges_test() ->
    DistinctValues = [crypto:rand_bytes(128) || _ <- lists:seq(1, 10*1000)],
    DistinctCount = length(DistinctValues),

    Hyper = lists:foldl(fun (V, H) ->
                                add(V, H)
                        end, new(15), DistinctValues),
    error_logger:info_msg("true distinct: ~p, estimated: ~p~n",
                          [DistinctCount, card(Hyper)]).

    
