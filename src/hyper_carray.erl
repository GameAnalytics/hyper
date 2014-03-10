%% @doc
%% Dummy module for the NIF implementation in {@link /c_src/hyper_carray.c}.
-module (hyper_carray).

-on_load(load_shared_obj/0).

-behaviour(hyper_register).
-export([new/1,
         set/3,
         max_merge/1,
         max_merge/2,
         bytes/1,
         register_sum/1,
         zero_count/1,
         encode_registers/1,
         decode_registers/2,
         compact/1]).


% Pull in the NIF.
load_shared_obj() ->
    EbinDir = filename:dirname(code:which(?MODULE)),
    AppPath = filename:dirname(EbinDir),
    Path = filename:join([AppPath, "priv", "hyper_carray"]),
    ok = erlang:load_nif(Path, 0).


% Nothing to do on compaction.
compact(Handle) ->
    Handle.


% Delegate to generalization.
max_merge(HandleA, HandleB) ->
    max_merge([HandleA, HandleB]).


new(_Precision) ->
    erlang:nif_error(nif_not_loaded).


set(_Index, _Value, _Handle) ->
    erlang:nif_error(nif_not_loaded).


max_merge(_Handles) ->
    erlang:nif_error(nif_not_loaded).


register_sum(_Handle) ->
    erlang:nif_error(nif_not_loaded).


zero_count(_Handle) ->
    erlang:nif_error(nif_not_loaded).


encode_registers(_Handle) ->
    erlang:nif_error(nif_not_loaded).


decode_registers(_Binary, _Precision) ->
    erlang:nif_error(nif_not_loaded).


bytes(_Handle) ->
    erlang:nif_error(nif_not_loaded).
