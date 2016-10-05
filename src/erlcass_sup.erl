-module(erlcass_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = [proccess(erlcass, infinity)],
    {ok, {{one_for_one, 10, 10}, Children}}.

proccess(Name, WaitForClose) ->
    {Name, {Name, start_link, []}, permanent, WaitForClose, worker, [Name]}.