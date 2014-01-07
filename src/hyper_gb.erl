-module(hyper_gb).
-export([new/1, get/2, set/3, fold/3]).
-behaviour(hyper_register).

new(_P) ->
    gb_trees:empty().

get(Index, T) ->
    case gb_trees:lookup(Index, T) of
        {value, V} ->
            {ok, V};
        none ->
            undefined
    end.

set(Index, Value, T) ->
    gb_trees:enter(Index, Value, T).



fold(F, A, {_, T}) when is_function(F, 3) ->
    fold_1(F, A, T).

fold_1(F, Acc0, {Key, Value, Small, Big}) ->
    Acc1 = fold_1(F, Acc0, Small),
    Acc = F(Key, Value, Acc1),
    fold_1(F, Acc, Big);
fold_1(_, Acc, _) ->
    Acc.
