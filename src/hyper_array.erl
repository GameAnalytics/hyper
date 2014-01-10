-module(hyper_array).
-export([new/1, set/3, fold/3, max_merge/2, bytes/1]).
-export([register_sum/1, zero_count/1, encode_registers/1, decode_registers/2, compact/1]).
-behaviour(hyper_register).

new(P) ->
    M = trunc(math:pow(2, P)),
    array:new([{size, M}, {fixed, true}, {default, 0}]).

set(Index, Value, A) ->
    case array:get(Index, A) of
        R when R > Value ->
            A;
        _ ->
            array:set(Index, Value, A)
    end.

fold(F, Acc, A) ->
    array:sparse_foldl(F, Acc, A).

max_merge(Left, Right) ->
    fold(fun (Index, L, Registers) ->
                 case array:get(Index, Registers) of
                     R when R < L ->
                         set(Index, L, Registers);
                     _ ->
                         Registers
                 end
         end, Right, Left).

bytes(A) ->
    erts_debug:flat_size(A) * 8.

register_sum(A) ->
    array:foldl(fun (_, Value, Sum) ->
                               Sum + math:pow(2, -Value)
                       end, 0, A).

zero_count(A) ->
    array:foldl(fun (_, 0, Sum) -> Sum + 1;
                    (_, _, Sum) -> Sum
                end, 0, A).

compact(A) ->
    A.

encode_registers(A) ->
    iolist_to_binary(
      lists:reverse(
        array:foldl(fun (_, V, Acc) -> [<<V:8/integer>> | Acc] end,
                    [], A))).

decode_registers(Bytes, P) ->
    do_decode_registers(Bytes, 0, new(P)).

do_decode_registers(<<>>, _, A) ->
    A;
do_decode_registers(<<Value:8/integer, Rest/binary>>, I, A) ->
    NewA = case Value of
               0 -> A;
               N -> array:set(I, N, A)
           end,
    do_decode_registers(Rest, I+1, NewA).
