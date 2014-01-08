-module(hyper_bisect).
-include_lib("eunit/include/eunit.hrl").

-export([new/1, get/2, set/3, fold/3, max_merge/2]).
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
    case catch binary:at(B, Index) of
        {'EXIT', _} ->
            error_logger:info_msg("dense size: ~p, index: ~p~n", [byte_size(B), Index]);
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
    InterfaceF = fun (<<Index:?KEY_SIZE/integer>>, <<Value:?VALUE_SIZE/integer>>, A) ->
                         F(Index, Value, A)
                 end,
    bisect:foldl(B, InterfaceF, Acc);
fold(F, Acc, {dense, B}) ->
    do_dense_fold(F, Acc, B).


max_merge({sparse, Left, P, T}, {sparse, Right, P, T}) ->
    {sparse, bisect:merge(fun (_Index, L, R) -> max(L, R) end, Left, Right), P, T};

max_merge({dense, Left}, {dense, Right}) ->
    {dense, iolist_to_binary(
              lists:reverse(
                do_dense_merge(Left, Right)))};

max_merge({dense, Dense}, {sparse, Sparse, _, _}) ->
    do_dense_sparse_merge({dense, Dense}, bisect:to_orddict(Sparse));

max_merge({sparse, Sparse, _, _}, {dense, Dense}) ->
    do_dense_sparse_merge({dense, Dense}, bisect:to_orddict(Sparse)).



%%
%% INTERNAL
%%

do_dense_merge(<<>>, <<>>) ->
    [];
do_dense_merge(<<Left, LeftRest/binary>>, <<Right, RightRest/binary>>) ->
    [max(Left, Right) | do_dense_merge(LeftRest, RightRest)].

do_dense_sparse_merge({dense, Dense}, []) ->
    {dense, Dense};
do_dense_sparse_merge({dense, Dense}, [{<<Index:?KEY_SIZE/integer>>,
                                        <<Value:?VALUE_SIZE/integer>>} | Rest]) ->
    do_dense_sparse_merge(set(Index, Value, {dense, Dense}), Rest).

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
