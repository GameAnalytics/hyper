-module(hyper_register).

-callback new(P :: hyper:precision())                  -> hyper:registers().
-callback set(Index :: integer(), Value :: integer(),
              hyper:registers())                       -> hyper:registers().
-callback max_merge(hyper:registers(), hyper:registers()) -> hyper:registers().
    






