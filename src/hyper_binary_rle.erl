-module(hyper_binary_rle).
-include_lib("eunit/include/eunit.hrl").

-export([new/1, set/3, compact/1, max_merge/1, max_merge/2, bytes/1]).
-export([register_sum/1, zero_count/1, encode_registers/1, decode_registers/2]).

-behaviour(hyper_register).

-define(KEY_SIZE, 16).
-define(REPEAT, 16/integer).
-define(REPEAT(R), (R-1):16/integer).
-define(VALUE, 8/integer).
-define(VALUE(V), V:8/integer).
-define(MARKER, "rle").


new(P) ->
    {rle, <<?MARKER, (m(P)-1):?REPEAT, 0:?VALUE>>}.


set(Index, Value, {rle, B}) ->
    New = rle_insert(B, Index, Value),
    {rle, New}.


compact({rle, B}) ->
    {rle, do_compact(B)}.

max_merge([First | Rest]) ->
    lists:foldl(fun (B, Acc) ->
                        max_merge(B, Acc)
                end, First, Rest).


bytes({rle, B}) ->
    erlang:byte_size(B).


register_sum(B) ->
    foldl(fun (Count, Value, Acc) -> Acc + (math:pow(2, -Value) * Count) end,
          0, B).

zero_count(B) ->
    foldl(fun (Count, 0, Acc) -> Acc + Count;
              (_,     _, Acc) -> Acc end,
          0, B).

encode_registers({rle, B}) ->
    iolist_to_binary(encode_rle(B)).

decode_registers(AllBytes, P) ->
    M = m(P),
    Bytes = case AllBytes of
                <<B:M/binary>>    -> B;
                <<B:M/binary, 0>> -> B
            end,
    {rle, bytes2rle(Bytes)}.


max_merge(Small, Big) ->
    do_max_merge(Small, Big).



%%
%% INTERNALS
%%

m(P) ->
    trunc(math:pow(2, P)).

foldl(F, Acc, {rle, <<?MARKER, B/binary>>}) ->
    do_foldl(F, Acc, B).

do_foldl(F, Acc, B) ->
    case take_repeat(B) of
        {Count, Value, Rest} ->
            do_foldl(F, F(Count, Value, Acc), Rest);
        undefined ->
            Acc
    end.



encode_rle(<<?MARKER, B/binary>>) ->
    encode_rle(B);
encode_rle(<<>>) ->
    [];
encode_rle(B) ->
    {Repeats, Value, Rest} = take_repeat(B),
    [binary:copy(<<Value:?VALUE>>, Repeats) | encode_rle(Rest)].



bytes2rle(<<First:?VALUE, Rest/binary>>) ->
    iolist_to_binary([?MARKER | bytes2rle(Rest, First, 1)]).

bytes2rle(<<Value:?VALUE, Rest/binary>>, Value, RepeatCount) ->
    bytes2rle(Rest, Value, RepeatCount+1);
bytes2rle(<<Value:?VALUE, Rest/binary>>, RepeatedValue, RepeatCount) ->
    [<<?REPEAT(RepeatCount), ?VALUE(RepeatedValue)>> | bytes2rle(Rest, Value, 1)];
bytes2rle(<<>>, RepeatedValue, RepeatCount) ->
    [<<?REPEAT(RepeatCount), ?VALUE(RepeatedValue)>>].


do_compact(<<?MARKER, B/binary>>) ->
    case take_repeat(B) of
        {FirstCount, FirstValue, Rest} ->
            iolist_to_binary([?MARKER | do_compact(Rest, FirstCount, FirstValue)])
    end.

do_compact(B, PrevCount, PrevValue) ->
    case take_repeat(B) of
        {Count, PrevValue, Rest} ->
            do_compact(Rest, PrevCount + Count, PrevValue);
        {Count, Value, Rest}->
            [<<?REPEAT(PrevCount), ?VALUE(PrevValue)>>
                 | do_compact(Rest, Count, Value)];
        undefined ->
            [<<?REPEAT(PrevCount), ?VALUE(PrevValue)>>]
    end.

do_max_merge({rle, Small}, {rle, Big}) ->
    SmallEncoded = encode_registers({rle, Small}),
    BigEncoded   = encode_registers({rle, Big}),

    MergedEncoded = do_max_merge(SmallEncoded, BigEncoded),
    {rle, bytes2rle(iolist_to_binary(MergedEncoded))};

do_max_merge(<<Small:?VALUE, SmallRest/binary>>,
             <<Big:?VALUE, BigRest/binary>>) ->
    [max(Small, Big) | do_max_merge(SmallRest, BigRest)];
do_max_merge(<<>>, <<>>) ->
    [].





rle_insert(<<?MARKER, B/binary>>, Index, Value) ->
    <<?MARKER, (iolist_to_binary(rle_insert(B, 0, Index, Value)))/binary>>.

%% rle_insert(<<>>, I, Index, Value) ->
%%     throw(end_reached);

rle_insert(B, I, Index, Value) ->
    {Repeats, RepeatValue, Rest} = take_repeat(B),

    ChunkStart = I,
    ChunkEnd = ChunkStart + Repeats - 1,

    %% error_logger:info_msg("~p repeated ~p times, I: ~p, Index: ~p, Value: ~p~n"
    %%                       "chunk start: ~p, chunk end: ~p~n",
    %%                       [RepeatValue, Repeats, I, Index, Value, ChunkStart, ChunkEnd]),

    if
        %% Found the chunk where index and value belongs, doing
        %% nothing as the value is bigger than what we're inserting.
        ChunkStart =< Index andalso Index =< ChunkEnd
        andalso Value =< RepeatValue ->
            B;

        %% Found the chunk where index belongs, but it has a different
        %% value. Split the chunk into three new chunks.
        ChunkStart =< Index andalso Index =< ChunkEnd andalso
        RepeatValue =/= Value ->
            ChunkSize = ChunkEnd + 1 - ChunkStart,
            LeftFill = Index - ChunkStart,
            RightFill = ChunkEnd - Index,
            %% error_logger:info_msg("splitting chunk~n"),

            Left = if LeftFill =:= 0 -> <<>>;
                      true -> <<?REPEAT(LeftFill), RepeatValue:?VALUE>>
                   end,
            Right = if RightFill =:= 0 -> <<>>;
                       true -> <<?REPEAT(RightFill), RepeatValue:?VALUE>>
                    end,
            [Left, <<?REPEAT(1), ?VALUE(Value)>>, Right, Rest];

        ChunkEnd < Index ->
            %% error_logger:info_msg("chunk end < index (~p < ~p) recursing~n",
            %%                       [ChunkEnd, Index]),

            [<<?REPEAT(Repeats), ?VALUE(RepeatValue)>> |
             rle_insert(Rest, ChunkEnd+1, Index, Value)]
    end.

take_repeat(<<>>) ->
    undefined;
take_repeat(<<Repeats:?REPEAT, Value:?VALUE, Rest/binary>>) ->
    %% Repeats is zero based
    {Repeats+1, Value, Rest}.



%%
%% TESTS
%%

format({rle, <<?MARKER, B/binary>>}) ->
    rle_to_list(B).

rle_to_list(<<>>) ->
    [];
rle_to_list(B) ->
    {Repeats, Value, Rest} = take_repeat(B),
    [{Repeats, Value} | rle_to_list(Rest)].


set_test() ->
    P = 4, M = m(P), Empty = new(P),

    R1 = set(3, 3, Empty),
    ?assertEqual([{3, 0}, {1, 3}, {12, 0}], format(R1)),
    ?assertEqual(M, size(encode_registers(R1))),
    ?assertEqual(<<0, 0, 0, 3,
                   0, 0, 0, 0,
                   0, 0, 0, 0,
                   0, 0, 0, 0>>, encode_registers(R1)),

    R2 = set(5, 5, R1),
    ?assertEqual([{3, 0}, {1, 3}, {1, 0}, {1, 5}, {10, 0}], format(R2)),
    ?assertEqual(M, size(encode_registers(R2))),
    ?assertEqual(<<0, 0, 0, 3,
                   0, 5, 0, 0,
                   0, 0, 0, 0,
                   0, 0, 0, 0>>, encode_registers(R2)).


first_index_test() ->
    P = 4,
    Empty = new(P),
    R1 = set(0, 1, Empty),
    ?assertEqual([{1, 1}, {15, 0}], format(R1)),
    ?assertEqual(R1, set(0, 1, R1)).

last_index_test() ->
    P = 4,
    Empty = new(P),
    R1 = set(15, 1, Empty),
    ?assertEqual([{15, 0}, {1, 1}], format(R1)),
    ?assertEqual([{15, 0}, {1, 1}], format(set(15, 1, R1))),
    ?assertEqual(R1, set(15, 1, R1)).


p16_test() ->
    P = 16,
    Empty = new(P),
    R1 = set(0, 1, Empty),
    ?assertEqual([{1, 1}, {m(P) - 1, 0}], format(R1)),
    ?assertEqual(m(P), byte_size(encode_registers(R1))).

decode_test() ->
    Bytes = <<0,0,0,0,
              4,5,6,7,
              0,0,0,0,
              9,0,1,2>>,
    R = decode_registers(Bytes, 4),
    ?assertEqual(Bytes, encode_registers(R)).

encode_decode_test() ->
    P = 4, Empty = new(P),
    R1 = set(1, 10, Empty),
    ?assertEqual([{1, 0}, {1, 10}, {14, 0}], format(R1)),
    R2 = set(2, 10, R1),
    ?assertEqual([{1, 0}, {1, 10}, {1, 10}, {13, 0}], format(R2)),
    ?assertEqual([{1, 0}, {2, 10}, {13, 0}], format(compact(R2))),
    R3 = set(4, 10, R2),
    ?assertEqual([{1, 0}, {2, 10}, {1, 0}, {1, 10}, {11, 0}], format(compact(R3))).

merge_chunk_test() ->
    P = 4, Empty = new(P),
    R = set(2, 3, set(3, 3, set(1, 3, Empty))),
    ?assertEqual([{1, 0}, {3, 3}, {12, 0}], format(compact(R))).



proper_test_() ->
    {timeout, 30,
     fun () ->
             P = 4,
             L = [<<0,0,62,102,34,26,89,98>>,
                  <<0,0,29,166,217,88,10,37>>,
                  <<0,0,16,95,189,198,138,252>>,
                  <<0,0,24,97,85,214,149,172>>,
                  <<0,0,2,237,55,216,209,255>>,
                  <<0,0,22,223,10,247,83,142>>,
                  <<0,0,45,147,35,36,250,51>>,
                  <<0,0,7,122,146,137,108,208>>],
             H = lists:foldl(fun hyper:insert/2, hyper:new(P, hyper_binary_rle), L),
             {hyper, P, {_, R}} = H,
             ?assertEqual(format(compact(R)),
                          format(decode_registers(encode_registers(R), P))),
             ?assertEqual(compact(R),
                          decode_registers(encode_registers(R), P))
     end}.
    






