-module(erlcass_app).

-behaviour(application).

-include("erlcass.hrl").
-include("erlcass_internals.hrl").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->

    ok = erlcass_cluster:create(),
    erlcass_cluster:set_log_level(erlcass_utils:get_env(log_level, ?CASS_LOG_WARN)),

    case erlcass_utils:get_env(cluster_options) of
        {ok, ClusterOptions} ->
            erlcass_cluster:set_options(ClusterOptions);
        _ ->
            ok
    end,

    ok = erlcass_stm_cache:create(),
    erlcass_sup:start_link().

stop(_State) ->
    ok = erlcass_cluster:release(),
    ok.