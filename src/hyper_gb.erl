-module(hyper_gb).
-export([new/1, get/2, set/3, fold/3, max_merge/2, bytes/1]).
-export([register_sum/1, zero_count/1, encode_registers/1, decode_registers/2, compact/1]).
-include_lib("eunit/include/eunit.hrl").

-behaviour(hyper_register).

new(P) ->
    {gb_trees:empty(), trunc(math:pow(2, P))}.

get(Index, {T, _M}) ->
    case gb_trees:lookup(Index, T) of
        {value, V} ->
            {ok, V};
        none ->
            undefined
    end.

set(Index, Value, {T, M}) ->
    {gb_trees:enter(Index, Value, T), M}.

max_merge(Small, Big) ->
    fold(fun (Index, L, Registers) ->
                 case get(Index, Registers) of
                      {ok, R} when R < L ->
                          set(Index, L, Registers);
                      {ok, _} ->
                          Registers;
                      undefined ->
                          set(Index, L, Registers)
                  end
          end, Big, Small).

fold(F, A, {{_, T}, _M}) when is_function(F, 3) ->
    fold_1(F, A, T).

fold_1(F, Acc0, {Key, Value, Small, Big}) ->
    Acc1 = fold_1(F, Acc0, Small),
    Acc = F(Key, Value, Acc1),
    fold_1(F, Acc, Big);
fold_1(_, Acc, _) ->
    Acc.

bytes({T, _}) ->
    erts_debug:flat_size(T) * 8.


register_sum({T, M}) ->
    {MaxI, Sum} = fold(fun (Index, Value, {I, Acc}) ->
                            Zeroes = Index - I - 1,
                            {Index, Acc + math:pow(2, -Value) +
                                 (math:pow(2, -0) * Zeroes)}
                    end, {-1, 0}, {T, M}),
    Sum + (M - 1 - MaxI) * math:pow(2, -0).


zero_count({T, M}) ->
    M - gb_trees:size(T).

compact(T) ->
    T.

encode_registers({T, M}) ->
    iolist_to_binary(
      encode_registers(M-1, T, [])).

encode_registers(I, _T, ByteEncoded) when I < 0 ->
    ByteEncoded;

encode_registers(I, T, ByteEncoded) when I >= 0 ->
    Byte = case gb_trees:lookup(I, T) of
               {value, V} ->
                   <<V:8/integer>>;
               none ->
                   <<0>>
           end,
    encode_registers(I - 1, T, [Byte | ByteEncoded]).


decode_registers(Bytes, P) ->
    L = do_decode_registers(Bytes, 0),
    T = gb_trees:from_orddict(L),
    M = trunc(math:pow(2, P)),
    {T, M}.


do_decode_registers(<<>>, _) ->
    [];
do_decode_registers(<<0:8/integer, Rest/binary>>, I) ->
    do_decode_registers(Rest, I+1);
do_decode_registers(<<Value:8/integer, Rest/binary>>, I) ->
    [{I, Value} | do_decode_registers(Rest, I+1)].

%%
%% TESTS
%%

sum_test() ->
    T = set(3, 5, set(1, 1, new(4))),

    ?assertEqual(lists:sum([
                            math:pow(2, -0), % 0
                            math:pow(2, -1), % 1
                            math:pow(2, -0), % 2
                            math:pow(2, -5), % 3
                            math:pow(2, -0), % 4
                            math:pow(2, -0), % 5
                            math:pow(2, -0), % 6
                            math:pow(2, -0), % 7
                            math:pow(2, -0), % 8
                            math:pow(2, -0), % 9
                            math:pow(2, -0), % 10
                            math:pow(2, -0), % 11
                            math:pow(2, -0), % 12
                            math:pow(2, -0), % 13
                            math:pow(2, -0), % 14
                            math:pow(2, -0)  % 5
                           ]),
                 register_sum(T)).

zero_test() ->
    P = 4, M = 16,
    T = set(3, 5, set(1, 1, new(P))),
    ?assertEqual(M - 2, zero_count(T)).
