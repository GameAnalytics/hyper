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


% Nothing to do.
compact(Handle) ->
    Handle.


% Delegate to generalization.
max_merge(HandleA, HandleB) ->
    max_merge([HandleA, HandleB]).


% Returns badarg on:
%   - Precision is not a positive integer
new(_Precision) ->
    erlang:nif_error(nif_not_loaded).

% Returns badarg on:
%   - Index is not a positive integer
%   - Value not a positive integer
%   - Handle is not a hyper_carray
% Value is assumed to fit into one byte, that is within [0, 255]
set(_Index, _Value, _Handle) ->
    erlang:nif_error(nif_not_loaded).


% Returns badarg on:
%   - Argument is not a list
%   - Argument is a list of less than 2 elements
%   - An element is not a hyper_carray
%   - The elements lack uniform precision
%   - Failure to obtain a list element (enif_get_list_cell)
max_merge(_Handles) ->
    erlang:nif_error(nif_not_loaded).


% Returns badarg on:
%   - Handle is not a hyper_carray
register_sum(_Handle) ->
    erlang:nif_error(nif_not_loaded).


% Returns badarg on:
%   - Handle is not a hyper_carray
zero_count(_Handle) ->
    erlang:nif_error(nif_not_loaded).


% Returns badarg on:
%   - Handle is not a hyper_carray
encode_registers(_Handle) ->
    erlang:nif_error(nif_not_loaded).


% Returns badarg on:
%   - First argument is not a binary
%   - Precision is not a positive integer
decode_registers(_Binary, _Precision) ->
    erlang:nif_error(nif_not_loaded).


% Returns badarg on:
%   - Handle is not a hyper_carray
bytes(_Handle) ->
    erlang:nif_error(nif_not_loaded).
