-module(erlcass_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    init/1
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, Clusters} = erlcass_utils:get_env(clusters),

    Logger = [
        {erlcass_log, {erlcass_log, start_link, []}, permanent, infinity, worker, [erlcass_log]}
    ],
    Workers = lists:map(fun(Cluster) -> process(Cluster, infinity) end, Clusters),
    Children = Logger ++ Workers,

    {ok, {{one_for_one, 10, 1}, Children}}.

process({Name, Config}, WaitForClose) ->
    {Name, {erlcass, start_link, [{Name, Config}]}, permanent, WaitForClose, worker, [erlcass]}.