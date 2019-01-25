-module(erlcass_cluster).

-export([
    create/0,
    set_options/2,
    set_log_level/1,
    set_log_process_receiver/1,
    release/1
]).

create() ->
    {ok, _Cluster} = erlcass_nif:cass_cluster_create().

set_options(Cluster, Options) ->
    ok = erlcass_nif:cass_cluster_set_options(Cluster, Options).

set_log_level(Level) ->
    ok = erlcass_nif:cass_log_set_level(Level).

set_log_process_receiver(Pid) ->
    ok = erlcass_nif:cass_log_set_callback(Pid).

release(Cluster) ->
    erlcass_nif:cass_cluster_release(Cluster).