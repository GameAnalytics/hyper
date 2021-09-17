%% @doc: If you wish to implement your own backend for storing
%% registers, your module needs to implement these interfaces. The
%% backend modules have quite a lot of responsibility (detailed below)
%% to allow for backend-specific optimizations.
-module(hyper_register).

-type t() :: module().

-export_type([t/0]).

-export([new/2, set/4, compact/2, max_merge/2, max_merge/3, reduce_precision/3,
         register_sum/2, zero_count/2, encode_registers/2, decode_registers/3, bytes/2]).

%% @doc: Creates a new instance of the backend. The return value of
%% this function will be passed to all functions in this module.
-callback new(P :: hyper:precision()) ->
    hyper:registers().

%% @doc: Set the register to the given value, *only* if the value
%% already stored is lower than the new value. The backend needs to
%% ensure the register value is only allowed to increase.
-callback set(Index :: integer(),
              Value :: integer(),
              hyper:registers()) ->
    hyper:registers().

%% @doc: Compact is always called before any attempt at reading (sum,
%% zero count, etc) or merging. It is intended to give backends that
%% buffer the writes a chance to flush the buffer before the registers
%% are needed.
-callback compact(hyper:registers()) ->
    hyper:registers().


%% @doc: Merge any number of registers, used to calculate the
%% union. For two register values at the same index, the max value
%% must be in the resulting register.
-callback max_merge([hyper:registers()]) ->
    hyper:registers().

%% @doc: Same as max_merge/1 but used when we know only two filters
%% are merged.
-callback max_merge(hyper:registers(),
                    hyper:registers()) ->
    hyper:registers().

%% @doc: Reduce the precision of the registers. Used for mixed-precision
%% union by first reducing the precision to the lowest of all filters.
-callback reduce_precision(hyper:precision(),
                           hyper:registers()) ->
    hyper:registers().

%% @doc: Sum of 2^-R where R is the value in each register.
-callback register_sum(hyper:registers()) ->
    float().

%% @doc: Count of registers set to 0.
-callback zero_count(hyper:registers()) ->
    integer().

%% @doc: Encode and decode are called to convert the in-memory
%% representation of the backend to the serialized format. Must return
%% one binary where each register is encoded as an 8-bit integer.
-callback encode_registers(hyper:registers()) ->
    binary().

-callback decode_registers(binary(), hyper:precision()) ->
    hyper:registers().

%% @doc: Size in bytes used to represent the registers in memory.
-callback bytes(hyper:registers()) ->
    integer().

%% @doc: Creates a new instance of the backend. The return value of
%% this function will be passed to all functions in this module.
-spec new(t(), P :: hyper:precision()) -> hyper:registers().
new(RegisterImpl, P) ->
    RegisterImpl:new(P).

%% @doc: Set the register to the given value, *only* if the value
%% already stored is lower than the new value. The backend needs to
%% ensure the register value is only allowed to increase.
-spec set(t(), Index :: integer(), Value :: integer(), hyper:registers()) ->
             hyper:registers().
set(RegisterImpl, Index, Value, Registers) ->
    RegisterImpl:set(Index, Value, Registers).

%% @doc: Compact is always called before any attempt at reading (sum,
%% zero count, etc) or merging. It is intended to give backends that
%% buffer the writes a chance to flush the buffer before the registers
%% are needed.
-spec compact(t(), hyper:registers()) -> hyper:registers().
compact(RegisterImpl, Registers) ->
    RegisterImpl:compact(Registers).

%% @doc: Merge any number of registers, used to calculate the
%% union. For two register values at the same index, the max value
%% must be in the resulting register.
-spec max_merge(t(), [hyper:registers()]) -> hyper:registers().
max_merge(RegisterImpl, RegisterLists) ->
    RegisterImpl:max_merge(RegisterLists).

%% @doc: Same as max_merge/1 but used when we know only two filters
%% are merged.
-spec max_merge(t(), hyper:registers(), hyper:registers()) -> hyper:registers().
max_merge(RegisterImpl, Registers1, Registers2) ->
    RegisterImpl:max_merge(Registers1, Registers2).

%% @doc: Reduce the precision of the registers. Used for mixed-precision
%% union by first reducing the precision to the lowest of all filters.
-spec reduce_precision(t(), hyper:precision(), hyper:registers()) -> hyper:registers().
reduce_precision(RegisterImpl, Precision, Registers) ->
    RegisterImpl:reduce_precision(Precision, Registers).

%% @doc: Sum of 2^-R where R is the value in each register.
-spec register_sum(t(), hyper:registers()) -> float().
register_sum(RegisterImpl, Registers) ->
    RegisterImpl:register_sum(Registers).

%% @doc: Count of registers set to 0.
-spec zero_count(t(), hyper:registers()) -> integer().
zero_count(RegisterImpl, Registers) ->
    RegisterImpl:zero_count(Registers).

%% @doc: Encode and decode are called to convert the in-memory
%% representation of the backend to the serialized format. Must return
%% one binary where each register is encoded as an 8-bit integer.
-spec encode_registers(t(), hyper:registers()) -> binary().
encode_registers(RegisterImpl, Registers) ->
    RegisterImpl:encode_registers(Registers).

-spec decode_registers(t(), binary(), hyper:precision()) -> hyper:registers().
decode_registers(RegisterImpl, Encoded, Precision) ->
    RegisterImpl:decode_registers(Encoded, Precision).

%% @doc: Size in bytes used to represent the registers in memory.
-spec bytes(t(), hyper:registers()) -> integer().
bytes(RegisterImpl, Registers) ->
    RegisterImpl:bytes(Registers).
