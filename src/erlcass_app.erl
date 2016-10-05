-module(erlcass_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = erlcass_stm_cache:create(),
    erlcass_sup:start_link().

stop(_State) ->
    ok.