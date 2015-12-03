-module(erlcass_uuid).
-author("silviu.caragea").

-export([
    gen_time/0,
    gen_random/0,
    gen_from_ts/1,
    min_from_ts/1,
    max_from_ts/1,
    get_ts/1,
    get_version/1
]).

-export([start_link/0, init/1]).

-define(PROC_NAME, erlcass_uuid_proc).

start_link() ->
    proc_lib:start_link(?MODULE, init, [self()]).

-spec(gen_time() ->
    {ok, Uuid :: binary()} | {error, Reason :: binary()}).

gen_time() ->
    send(gen_time).

-spec(gen_random() ->
    {ok, Uuid :: binary()} | {error, Reason :: binary()}).

gen_random() ->
    send(gen_random).

-spec(gen_from_ts(Ts :: integer()) ->
    {ok, Uuid :: binary()} | badarg | {error, Reason :: binary()}).

gen_from_ts(Ts) ->
    send(gen_from_time, Ts).

-spec(min_from_ts(Ts :: integer()) ->
    {ok, Uuid :: binary()} | badarg | {error, Reason :: binary()}).

min_from_ts(Ts) ->
    erlcass_nif:cass_uuid_min_from_time(Ts).

-spec(max_from_ts(Ts :: integer()) ->
    {ok, Uuid :: binary()} | badarg | {error, Reason :: binary()}).

max_from_ts(Ts) ->
    erlcass_nif:cass_uuid_max_from_time(Ts).

-spec(get_ts(Uuid :: binary()) ->
    {ok, Ts :: integer()} | badarg | {error, Reason :: binary()}).

get_ts(Uuid) ->
    erlcass_nif:cass_uuid_timestamp(Uuid).

-spec(get_version(Uuid :: binary()) ->
    {ok, Version :: integer()} | badarg | {error, Reason :: binary()}).

get_version(Uuid) ->
    erlcass_nif:cass_uuid_version(Uuid).

%private

init(Parent) ->
    true = register(?PROC_NAME, self()),
    {ok, UuidGenerator} = erlcass_nif:cass_uuid_gen_new(),
    proc_lib:init_ack(Parent, {ok, self()}),
    loop(UuidGenerator).

send(Tag) ->
    ?PROC_NAME ! {self(), Tag},

    receive
        {Tag, R} ->
            R
    end.

send(Tag, Arg) ->
    ?PROC_NAME ! {self(), {Tag, Arg}},

    receive
        {Tag, R} ->
            R
    end.

loop(UuidGenerator) ->
    receive
        {From, gen_time} ->
            From ! {gen_time, erlcass_nif:cass_uuid_gen_time(UuidGenerator)};
        {From, gen_random} ->
            From ! {gen_random, erlcass_nif:cass_uuid_gen_random(UuidGenerator)};
        {From, {gen_from_time, Ts}} ->
            From ! {gen_from_time, erlcass_nif:cass_uuid_gen_from_time(UuidGenerator, Ts)};
        _ ->
            ok
    end,

    loop(UuidGenerator).