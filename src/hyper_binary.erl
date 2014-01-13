-module(hyper_binary).
-include_lib("eunit/include/eunit.hrl").

-export([new/1, set/3, fold/3, compact/1, max_merge/1, max_merge/2, bytes/1]).
-export([register_sum/1, zero_count/1, encode_registers/1, decode_registers/2]).

-behaviour(hyper_register).

-define(KEY_SIZE, 16).
-define(REPEAT_SIZE, 16).
-define(VALUE_SIZE, 8).
-define(MARKER, "rle").

%% TODO: rle

new(P) ->
    M = trunc(math:pow(2, P)),
    {dense, binary:copy(<<0>>, M), [], 0}.

set(Index, Value, {dense, B, Tmp, TmpCount}) ->
    case binary:at(B, Index) of
        R when R < Value ->
            New = {dense, B, [{Index, Value} | Tmp], TmpCount + 1},
            case TmpCount < 100 of
                true ->
                    New;
                false ->
                    compact(New)
            end;
        _ ->
            {dense, B, Tmp, TmpCount}
    end.


compact({dense, B, Tmp, _TmpCount}) ->
    MaxR = lists:foldl(fun ({I, V}, Acc) ->
                               case orddict:find(I, Acc) of
                                   {ok, R} when R >= V ->
                                       Acc;
                                   _ ->
                                       orddict:store(I, V, Acc)
                               end
                       end, orddict:new(), Tmp),
    NewB = merge_tmp(B, MaxR),
    {dense, NewB, [], 0}.

fold(F, Acc, {dense, B, [], _}) ->
    do_fold(F, Acc, B).

max_merge([First | Rest]) ->
    lists:foldl(fun (B, Acc) ->
                        max_merge(B, Acc)
                end, First, Rest).

max_merge({dense, Small, [], _}, {dense, Big, [], _}) ->
    Merged = iolist_to_binary(do_merge(Small, Big)),
    {dense, Merged, [], 0}.

bytes({dense, B, _, _}) ->
    erlang:byte_size(B).


register_sum(B) ->
    fold(fun (_, V, Acc) -> Acc + math:pow(2, -V) end, 0, B).

zero_count(B) ->
    fold(fun (_, 0, Acc) -> Acc + 1;
             (_, _, Acc) -> Acc
         end, 0, B).

encode_registers({dense, B, _, _}) ->
    B.

decode_registers(Bytes, P) ->
    M = m(P),
    case Bytes of
        <<B:M/binary>> ->
            {dense, B, [], 0};
        <<B:M/binary, 0>> ->
            {dense, B, [], 0}
    end.


%%
%% INTERNALS
%%

m(P) ->
    trunc(math:pow(2, P)).

do_merge(<<>>, <<>>) ->
    [];
do_merge(<<Left:?VALUE_SIZE, SmallRest/binary>>,
         <<Right:?VALUE_SIZE, BigRest/binary>>) ->
    [max(Left, Right) | do_merge(SmallRest, BigRest)].

do_fold(F, Acc, B) ->
    do_fold(F, Acc, B, 0).

do_fold(_, Acc, <<>>, _) ->
    Acc;
do_fold(F, Acc, <<Value:?VALUE_SIZE/integer, Rest/binary>>, Index) ->
    do_fold(F, F(Index, Value, Acc), Rest, Index+1).


merge_tmp(B, L) ->
    iolist_to_binary(
      lists:reverse(
        merge_tmp(B, lists:sort(L), -1, []))).

merge_tmp(B, [], _PrevIndex, Acc) ->
    [B | Acc];

merge_tmp(B, [{Index, Value} | Rest], PrevIndex, Acc) ->
    I = Index - PrevIndex - 1,

    case B of
        <<Left:I/binary, _:?VALUE_SIZE/integer, Right/binary>> ->
            NewAcc = [<<Value:?VALUE_SIZE/integer>>, Left | Acc],
            %% error_logger:info_msg("B: ~p~nindex: ~p, previndex: ~p, i: ~p~n"
            %%                       "left: ~p~nright: ~p, right size: ~p~n"
            %%                       "new acc: ~p~n",
            %%                       [B, Index, PrevIndex, I, Left, Right,
            %%                        byte_size(Right),
            %%                        iolist_to_binary(lists:reverse(NewAcc))]),
            merge_tmp(Right, Rest, Index, NewAcc);
        <<Left:I/binary>> ->
            [<<Value:?VALUE_SIZE/integer>>, Left | Acc]
    end.


%%
%% TESTS
%%


merge_test() ->
    Tmp1 = [{1, 1},
            {3, 3},
            {9, 3},
            {15, 15}],
    Tmp2 = [{3, 5},
            {9, 2},
            {10, 5}],

    {dense, NewB, [], 0} = new(4),
    {dense, Compact, [], 0} = compact({dense, NewB, Tmp1, length(Tmp1)}),
    {dense, Compact2, [], 0} = compact({dense, Compact, Tmp2, length(Tmp2)}),

    ?assertEqual(<<0, 1, 0, 5,
                   0, 0, 0, 0,
                   0, 2, 5, 0,
                   0, 0, 0, 15>>, Compact2).


serialize_test() ->
    H = compact(lists:foldl(fun (I, Acc) -> set(I, I, Acc) end,
                            new(4), lists:seq(0, 15))),
    {dense, B, _, _} = H,
    ?assertEqual(B, encode_registers(H)),
    ?assertEqual(H, decode_registers(encode_registers(H), 4)).
