-module(hyper_bisect).
-export([new/1, get/2, set/3, fold/3, max_merge/2]).
-behaviour(hyper_register).

new(_P) ->
    bisect:new(4, 1).

get(Index, B) ->
    case bisect:find(B, <<Index:32/integer>>) of
        not_found ->
            undefined;
        <<Value:8/integer>> ->
            {ok, Value}
    end.


set(Index, Value, B) ->
    bisect:insert(B, <<Index:32/integer>>, <<Value:8/integer>>).


fold(F, Acc, B) ->
    InterfaceF = fun (<<Index:32/integer>>, <<Value:8/integer>>, A) ->
                         F(Index, Value, A)
                 end,
    bisect:foldl(B, InterfaceF, Acc).

max_merge(Left, Right) ->
    bisect:merge(fun (_Index, L, R) -> max(L, R) end, Left, Right).
