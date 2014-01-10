-module(hyper_register).

-callback new(P :: hyper:precision())                  -> hyper:registers().
-callback get(Index :: integer(), hyper:registers())   -> {ok, integer()} | undefined.
-callback set(Index :: integer(), Value :: integer(),
              hyper:registers())                       -> hyper:registers().
-callback max_merge(hyper:registers(), hyper:registers()) -> hyper:registers().
    






