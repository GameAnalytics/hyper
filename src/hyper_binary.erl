-module(hyper_binary).
-include_lib("eunit/include/eunit.hrl").

-export([new/1, get/2, set/3, fold/3, compact/1, max_merge/1, max_merge/2, bytes/1]).
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
    NewTmp = orddict:store(Index, Value, Tmp),
    case TmpCount < 50 of
        true ->
            {dense, B, NewTmp, TmpCount+1};
        false ->
            {dense, merge_tmp(B, NewTmp), [], 0}
    end.

get(Index, {dense, B, Tmp, _TmpCount}) ->
    case binary:at(B, Index) of
        0 ->
            case orddict:find(Index, Tmp) of
                {ok, V} ->
                    {ok, V};
                error ->
                    undefined
            end;
        V ->
            {ok, V}
    end.

compact({dense, B, Tmp, _TmpCount}) ->
    {dense, merge_tmp(B, Tmp), [], 0}.

fold(F, Acc, {dense, B, Tmp, _}) ->
    do_fold(F, Acc, merge_tmp(B, Tmp)).

max_merge([First | Rest]) ->
    lists:foldl(fun (B, Acc) ->
                        max_merge(B, Acc)
                end, First, Rest).

max_merge({dense, Small, SmallTmp, _}, {dense, Big, BigTmp, _}) ->
    {dense, do_merge(merge_tmp(Small, SmallTmp), merge_tmp(Big, BigTmp)), [], 0}.

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

decode_registers(Bytes, _P) ->
    {dense, Bytes, [], 0}.


%%
%% INTERNALS
%%

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
    Buffered = lists:foldl(fun (I, Acc) -> set(I, I, Acc) end,
                           new(4), lists:seq(0, 15)),
    Compact = compact(Buffered),
    ?assertEqual(undefined, get(0, Compact)),
    ?assertEqual({ok, 3}, get(3, Compact)),
    ?assertEqual({ok, 15}, get(15, Compact)),
    ?assertError(badarg, get(16, Compact)).

serialize_test() ->
    H = compact(lists:foldl(fun (I, Acc) -> set(I, I, Acc) end,
                            new(4), lists:seq(0, 15))),
    {dense, B, _, _} = H,
    ?assertEqual(B, encode_registers(H)),
    ?assertEqual(H, decode_registers(encode_registers(H), 4)).

