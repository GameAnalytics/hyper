-module(hyper_bisect).
-include_lib("eunit/include/eunit.hrl").

-export([new/1, get/2, set/3, fold/3, max_merge/1, max_merge/2, bytes/1]).
-export([register_sum/1, zero_count/1, encode_registers/1, decode_registers/2, compact/1]).
-behaviour(hyper_register).

-define(KEY_SIZE, 16).
-define(VALUE_SIZE, 8).

%%
%% BEHAVIOUR
%%

new(P) ->
    DenseSize = trunc(math:pow(2, P)),
    EntrySize = (?KEY_SIZE + ?VALUE_SIZE) div 8,
    Threshold = DenseSize div EntrySize,
    {sparse, bisect:new(?KEY_SIZE div 8, ?VALUE_SIZE div 8), P, Threshold}.


get(Index, {sparse, B, _, _}) ->
    case bisect:find(B, <<Index:?KEY_SIZE/integer>>) of
        not_found ->
            undefined;
        <<Value:?VALUE_SIZE/integer>> ->
            {ok, Value}
    end;

get(Index, {dense, B}) ->
    case binary:at(B, Index) of
        0 ->
            undefined;
        V ->
            {ok, V}
    end.


set(Index, Value, {sparse, B, P, Threshold}) ->
    NewB = bisect:insert(B,
                         <<Index:?KEY_SIZE/integer>>,
                         <<Value:?VALUE_SIZE/integer>>),

    case bisect:num_keys(NewB) < Threshold of
        true ->
            {sparse, NewB, P, Threshold};
        false ->
            {dense, bisect2dense(NewB, P)}
    end;

set(Index, Value, {dense, B}) ->
    <<Left:Index/binary, _:?VALUE_SIZE, Right/binary>> = B,
    {dense, iolist_to_binary([Left, <<Value:?VALUE_SIZE/integer>>, Right])}.



fold(F, Acc, {sparse, B, _, _}) ->
    InterfaceF = fun (<<Index:?KEY_SIZE/integer>>,
                      <<Value:?VALUE_SIZE/integer>>,
                      A) ->
                         F(Index, Value, A)
                 end,
    bisect:foldl(B, InterfaceF, Acc);
fold(F, Acc, {dense, B}) ->
    do_dense_fold(F, Acc, B).


max_merge(Registers) ->
    [First | Rest] = Registers,
    lists:foldl(fun (R, Acc) ->
                        max_merge(R, Acc)
                end, First, Rest).


max_merge({sparse, Small, P, T}, {sparse, Big, P, T}) ->
    {sparse, bisect:merge(Small, Big), P, T};


max_merge({dense, Left}, {dense, Right}) ->
    {dense, iolist_to_binary(
              lists:reverse(
                do_dense_merge(Left, Right)))};

max_merge({dense, Dense}, {sparse, Sparse, P, _}) ->
    {dense, iolist_to_binary(
              lists:reverse(
                do_dense_merge(Dense, bisect2dense(Sparse, P))))};

max_merge({sparse, Sparse, P, _}, {dense, Dense}) ->
    {dense, iolist_to_binary(
              lists:reverse(
                do_dense_merge(Dense, bisect2dense(Sparse, P))))}.


compact(B) ->
    B.

bytes({sparse, Sparse, _, _}) -> bisect:size(Sparse);
bytes({dense, Dense})         -> erlang:byte_size(Dense).


register_sum(B) ->
    M = case B of
            {dense, Bytes} -> erlang:byte_size(Bytes);
            {sparse, _, P, _} -> trunc(math:pow(2, P))
        end,

    {MaxI, Sum} = fold(fun (Index, Value, {I, Acc}) ->
                               Zeros = Index - I - 1,
                               {Index, Acc + math:pow(2, -Value)
                                + (math:pow(2, -0) * Zeros)}
                       end, {-1, 0}, B),
    Sum + (M - 1 - MaxI) * math:pow(2, -0).

zero_count({dense, _} = B) ->
    fold(fun (_, 0, Acc) -> Acc + 1;
             (_, _, Acc) -> Acc
         end, 0, B);
zero_count({sparse, B, P, _}) ->
    M = trunc(math:pow(2, P)),
    M - bisect:num_keys(B).

encode_registers({dense, B}) ->
    B;
encode_registers({sparse, B, P, _}) ->
    M = trunc(math:pow(2, P)),
    iolist_to_binary(
      encode_registers(M-1, B, [])).


encode_registers(I, _B, ByteEncoded) when I < 0 ->
    ByteEncoded;

encode_registers(I, B, ByteEncoded) when I >= 0 ->
    Byte = case bisect:find(B, <<I:?KEY_SIZE/integer>>) of
               not_found ->
                   <<0>>;
               V ->
                   V % already encoded
           end,
    encode_registers(I - 1, B, [Byte | ByteEncoded]).



decode_registers(Bytes, P) ->
    DenseSize = trunc(math:pow(2, P)),
    EntrySize = (?KEY_SIZE + ?VALUE_SIZE) div 8,
    Threshold = DenseSize div EntrySize,

    L = do_decode_registers(Bytes, 0),
    case length(L) < Threshold of
        true ->
            B = bisect:new(?KEY_SIZE div 8, ?VALUE_SIZE div 8),
            {sparse, bisect:from_orddict(B, L), P, Threshold};
        false ->
            {dense, Bytes}
    end.

do_decode_registers(<<>>, _) ->
    [];
do_decode_registers(<<0, Rest/binary>>, I) ->
    do_decode_registers(Rest, I+1);
do_decode_registers(<<Value:?VALUE_SIZE, Rest/binary>>, I) ->
    [{<<I:?KEY_SIZE/integer>>, <<Value:?VALUE_SIZE/integer>>}
     | do_decode_registers(Rest, I+1)].


%%
%% INTERNAL
%%

do_dense_merge(<<>>, <<>>) ->
    [];
do_dense_merge(<<Left, LeftRest/binary>>, <<Right, RightRest/binary>>) ->
    [max(Left, Right) | do_dense_merge(LeftRest, RightRest)].


do_dense_sparse_merge({dense, Dense}, Bisect) ->
    bisect:foldl(Bisect, fun (<<Index:?KEY_SIZE/integer>>,
                              <<Value:?VALUE_SIZE/integer>>,
                              {dense, D}) ->
                                 set(Index, Value, {dense, D})
                         end, {dense, Dense}).

do_dense_fold(F, Acc, B) ->
    do_dense_fold(F, Acc, B, 0).

do_dense_fold(_, Acc, <<>>, _) ->
    Acc;
do_dense_fold(F, Acc, <<Value, Rest/binary>>, Index) ->
    do_dense_fold(F, F(Index, Value, Acc), Rest, Index+1).



bisect2dense(B, P) ->
    M =  trunc(math:pow(2, P)),
    {LastI, L} = lists:foldl(fun ({<<Index:?KEY_SIZE/integer>>, V}, {I, Acc}) ->
                                     Index < M orelse throw(index_too_high),

                                     Fill = case Index - I of
                                                0 ->
                                                    [];
                                                ToFill ->
                                                    binary:copy(<<0>>, ToFill - 1)
                                            end,

                                     {Index, [V, Fill | Acc]}

                             end, {-1, []}, bisect:to_orddict(B)),
    %% Fill last
    Filled = [binary:copy(<<0>>, M - LastI - 1) | L],

    iolist_to_binary(lists:reverse(Filled)).

%%
%% TESTS
%%

bisect2dense_test() ->
    P = 4,
    M = 16, % pow(2, P)

    {sparse, B, _, _} =
        lists:foldl(fun ({I, V}, Acc) ->
                            set(I, V, Acc)
                    end, new(P), [{2, 1}, {1, 1}, {4, 4}, {15, 5}]),

    Dense = bisect2dense(B, P),
    ?assertEqual(M, size(Dense)),


    Expected = <<0, 1, 1, 0,
                 4, 0, 0, 0,
                 0, 0, 0, 0,
                 0, 0, 0, 5>>,
    ?assertEqual(M, size(Expected)),
    ?assertEqual(Expected, Dense).

set_dense_test() ->
    P = 4,
    B = set(2, 2, {dense, binary:copy(<<0>>, trunc(math:pow(2, P)))}),
    ?assertEqual({ok, 2}, get(2, B)).
