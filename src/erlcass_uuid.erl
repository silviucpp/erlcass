-module(erlcass_uuid).

-include("erlcass.hrl").

-export([
    gen_time/0,
    gen_random/0,
    gen_from_ts/1,
    min_from_ts/1,
    max_from_ts/1,
    get_ts/1,
    get_version/1
]).

-spec gen_time() -> {ok, binary()} | {error, reason()}.

gen_time() ->
    erlcass_nif:cass_uuid_gen_time().

-spec gen_random() -> {ok, binary()} | {error, reason()}.

gen_random() ->
    erlcass_nif:cass_uuid_gen_random().

-spec gen_from_ts(integer()) -> {ok, binary()} | {error, reason()}.

gen_from_ts(Ts) ->
    erlcass_nif:cass_uuid_gen_from_time(Ts).

-spec min_from_ts(integer()) -> {ok, binary()} | {error, reason()}.

min_from_ts(Ts) ->
    erlcass_nif:cass_uuid_min_from_time(Ts).

-spec max_from_ts(integer()) -> {ok, binary()} | {error, reason()}.

max_from_ts(Ts) ->
    erlcass_nif:cass_uuid_max_from_time(Ts).

-spec get_ts(binary()) -> {ok, integer()} | {error, reason()}.

get_ts(Uuid) ->
    erlcass_nif:cass_uuid_timestamp(Uuid).

-spec get_version(binary()) -> {ok, integer()} | {error, reason()}.

get_version(Uuid) ->
    erlcass_nif:cass_uuid_version(Uuid).