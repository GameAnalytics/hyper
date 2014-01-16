%% @doc: If you wish to implement your own backend for storing
%% registers, your module needs to implement these interfaces. The
%% backend modules have quite a lot of responsibility (detailed below)
%% to allow for backend-specific optimizations.
-module(hyper_register).

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

%% @doc: Sum of 2^R where R is the value in each register.
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





