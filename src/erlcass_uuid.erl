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

-spec(gen_time() ->
    {ok, Uuid :: binary()} | {error, Reason :: binary()}).

gen_time() ->
    erlcass_nif:cass_uuid_gen_time().

-spec(gen_random() ->
    {ok, Uuid :: binary()} | {error, Reason :: binary()}).

gen_random() ->
    erlcass_nif:cass_uuid_gen_random().

-spec(gen_from_ts(Ts :: integer()) ->
    {ok, Uuid :: binary()} | badarg | {error, Reason :: binary()}).

gen_from_ts(Ts) ->
    erlcass_nif:cass_uuid_gen_from_time(Ts).

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