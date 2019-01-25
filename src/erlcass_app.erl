-module(erlcass_app).

-behaviour(application).

-include("erlcass.hrl").

-export([
    start/2,
    stop/1
]).

start(_StartType, _StartArgs) ->
    erlcass_cluster:set_log_level(erlcass_utils:get_env(log_level, ?CASS_LOG_WARN)),

    {ok, Clusters} = erlcass_utils:get_env(clusters),
    lists:map(fun({Name, _Config}) -> start_stm_caches(Name) end, Clusters),
    erlcass_sup:start_link().

stop(_State) ->
    ok = erlcass_cluster:release(),
    ok.

start_stm_caches(Name) ->
    TableName = erlcass_stm_cache:get_table_name(Name),
    ok = erlcass_stm_cache:create(TableName).
