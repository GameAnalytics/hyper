%% @doc: Registers stored in one large binary
%%
%% This backend uses one plain Erlang binary to store registers. The
%% cost of rebuilding the binary is amortized by keeping a buffer of
%% inserts to perform in the future.

-module(hyper_binary).
-include_lib("eunit/include/eunit.hrl").
-behaviour(hyper_register).
-compile(native).

-export([new/1,
         set/3,
         compact/1,
         max_merge/1,
         max_merge/2,
         bytes/1,
         register_sum/1,
         zero_count/1,
         encode_registers/1,
         decode_registers/2]).


-define(VALUE_SIZE, 6).
-define(MERGE_THRESHOLD, 0.05).

-record(buffer, {buf, buf_size, p, convert_threshold}).
-record(dense, {b, buf, buf_size, merge_threshold}).


new(P) ->
    new_buffer(P).

new_buffer(P) ->
    M = m(P),
    ConvertThreshold = M div (5 * 8), % 5 words for each entry
    #buffer{buf = [], buf_size = 0, p = P, convert_threshold = ConvertThreshold}.

new_dense(P) ->
    M = m(P),
    T = max(trunc(M * ?MERGE_THRESHOLD), 16),
    #dense{b = empty_binary(M), buf = [], buf_size = 0, merge_threshold = T}.


set(Index, Value, #buffer{buf = Buf, buf_size = BufSize} = Buffer) ->
    NewBuffer = Buffer#buffer{buf = [{Index, Value} | Buf],
                              buf_size = BufSize + 1},
    case NewBuffer#buffer.buf_size < NewBuffer#buffer.convert_threshold of
        true ->
            NewBuffer;
        false ->
            buffer2dense(NewBuffer)
    end;

set(Index, Value, #dense{buf = Buf, buf_size = BufSize} = Dense) ->
    LeftOffset = Index * ?VALUE_SIZE,
    <<_:LeftOffset/bitstring, R:?VALUE_SIZE/integer, _/bitstring>> = Dense#dense.b,

    if
        R < Value ->
            New = Dense#dense{buf = [{Index, Value} | Buf],
                              buf_size = BufSize + 1},
            case New#dense.buf_size < Dense#dense.merge_threshold of
                true ->
                    New;
                false ->
                    compact(New)
            end;
        true ->
            Dense
    end.


compact(#buffer{} = Buffer) ->
    Buffer;

compact(#dense{b = B, buf = Buf} = Dense) ->
    NewB = merge_buf(B, max_registers(Buf)),
    Dense#dense{b = NewB, buf = [], buf_size = 0}.



max_merge([First | Rest]) ->
    lists:foldl(fun (B, Acc) ->
                        max_merge(B, Acc)
                end, First, Rest).

max_merge(#dense{b = SmallB, buf = []}, #dense{b = BigB, buf = []} = Big) ->
    Merged = do_merge(SmallB, BigB, <<>>),
    Big#dense{b = Merged};

max_merge(#buffer{buf = Buf}, #dense{b = B} = Dense) ->
    Merged = merge_buf(B, max_registers(Buf)),
    Dense#dense{b = Merged};

max_merge(#dense{b = B} = Dense, #buffer{buf = Buf}) ->
    Merged = merge_buf(B, max_registers(Buf)),
    Dense#dense{b = Merged};

max_merge(#buffer{buf = LeftBuf}, #buffer{buf = RightBuf} = Right) ->
    Right#buffer{buf = max_registers(LeftBuf ++ RightBuf)}.




register_sum(B) ->
    fold(fun (_, 0, Acc) -> Acc + 1.0;
             (_, 1, Acc) -> Acc + 0.5;
             (_, 2, Acc) -> Acc + 0.25;
             (_, 3, Acc) -> Acc + 0.125;
             (_, 4, Acc) -> Acc + 0.0625;
             (_, 5, Acc) -> Acc + 0.03125;
             (_, 6, Acc) -> Acc + 0.015625;
             (_, 7, Acc) -> Acc + 0.0078125;
             (_, 8, Acc) -> Acc + 0.00390625;
             (_, 9, Acc) -> Acc + 0.001953125;
             (_, V, Acc) -> Acc + math:pow(2, -V) end,
         0, B).

zero_count(B) ->
    fold(fun (_, 0, Acc) -> Acc + 1;
             (_, _, Acc) -> Acc
         end, 0, B).

encode_registers(#buffer{} = Buffer) ->
    encode_registers(buffer2dense(Buffer));

encode_registers(#dense{b = B}) ->
    << <<I:8/integer>> || <<I:?VALUE_SIZE/integer>> <= B >>.

decode_registers(AllBytes, P) ->
    M = m(P),
    Bytes = case AllBytes of
                <<B:M/binary>>    -> B;
                <<B:M/binary, 0>> -> B
            end,
    Dense = new_dense(P),
    Dense#dense{b = << <<I:?VALUE_SIZE/integer>> || <<I:8>> <= Bytes >>}.


bytes(#dense{b = B}) ->
    erlang:byte_size(B);
bytes(#buffer{} = Buffer) ->
    erts_debug:flat_size(Buffer) * 8.



%%
%% INTERNALS
%%

m(P) ->
    trunc(math:pow(2, P)).

empty_binary(M) ->
    list_to_bitstring([<<0:?VALUE_SIZE/integer>> || _ <-  lists:seq(0, M-1)]).

max_registers(Tmp) ->
    lists:foldl(fun ({I, V}, Acc) ->
                          case orddict:find(I, Acc) of
                              {ok, R} when R >= V ->
                                  Acc;
                              _ ->
                                  orddict:store(I, V, Acc)
                          end
                  end, orddict:new(), lists:reverse(lists:sort(Tmp))).


buffer2dense(#buffer{buf = Buf, p = P}) ->
    Dense = new_dense(P),
    Merged = merge_buf(Dense#dense.b, max_registers(Buf)),
    Dense#dense{b = Merged}.


do_merge(<<>>, <<>>, Acc) ->
    Acc;
do_merge(<<Left:?VALUE_SIZE/integer, SmallRest/bitstring>>,
         <<Right:?VALUE_SIZE/integer, BigRest/bitstring>>,
         Acc) ->
    do_merge(SmallRest, BigRest, <<Acc/bits, (max(Left, Right)):?VALUE_SIZE>>).


fold(F, Acc, #buffer{} = Buffer) ->
    fold(F, Acc, buffer2dense(Buffer));

fold(F, Acc, #dense{b = B, buf = []}) ->
    do_fold(F, Acc, B, 0).

do_fold(_, Acc, <<>>, _) ->
    Acc;
do_fold(F, Acc, <<Value:?VALUE_SIZE/integer, Rest/bitstring>>, Index) ->
    do_fold(F, F(Index, Value, Acc), Rest, Index+1).


merge_buf(B, L) ->
    merge_buf(B, L, -1, <<>>).

merge_buf(B, [], _PrevIndex, Acc) ->
    <<Acc/bitstring, B/bitstring>>;

merge_buf(B, [{Index, Value} | Rest], PrevIndex, Acc) ->
    I = (Index - PrevIndex - 1) * ?VALUE_SIZE,
    case B of
        <<Left:I/bitstring, OldValue:?VALUE_SIZE/integer, Right/bitstring>> ->
            case OldValue < Value of
                true ->
                    NewAcc = <<Acc/bitstring,
                               Left/bitstring,
                               Value:?VALUE_SIZE/integer>>,
                    merge_buf(Right, Rest, Index, NewAcc);
                false ->
                    NewAcc = <<Acc/bitstring,
                               Left/bitstring,
                               OldValue:?VALUE_SIZE/integer>>,
                    merge_buf(Right, Rest, Index, NewAcc)
            end;

        <<Left:I/bitstring>> ->
            <<Acc/bitstring, Left/bitstring, Value:?VALUE_SIZE/integer>>
    end.


%%
%% TESTS
%%


merge_test() ->
    P = 4, M = m(P),
    Tmp1 = [{1, 1},
            {3, 3},
            {9, 3},
            {15, 15}],
    Tmp2 = [{3, 5},
            {9, 2},
            {10, 5}],

    {buffer, [], 0, T, _} = new(P),

    {dense, Compact, [], 0, _} =
        compact({dense, empty_binary(M), Tmp1, length(Tmp1), T}),

    {dense, Compact2, [], 0, _} =
        compact({dense, Compact, Tmp2, length(Tmp2), T}),

    Expected = [0, 1, 0, 5,
                0, 0, 0, 0,
                0, 3, 5, 0,
                0, 0, 0, 15],
    Ints = [I || <<I:?VALUE_SIZE/integer>> <= Compact2],

    ?assertEqual(Expected, Ints).



serialize_test() ->
    H = compact(lists:foldl(fun (I, Acc) -> set(I, I, Acc) end,
                            new(4), lists:seq(0, 15))),

    ?assertEqual(<<0,1,2,3,
                   4,5,6,7,
                   8,9,10,11,
                   12,13,14,15>>, encode_registers(H)),
    ?assertEqual(H, decode_registers(encode_registers(H), 4)).

max_registers_test() ->
    ?assertEqual([{3, 3}], max_registers([{3, 1}, {3, 2}, {3, 3}])).
