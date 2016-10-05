
-module(erlcass_cluster).
-author("silviu").

-export([create/0, set_options/1, set_log_level/1, set_log_process_receiver/1, release/0]).

create() ->
    ok = erlcass_nif:cass_cluster_create().

set_options(Options) ->
    ok = erlcass_nif:cass_cluster_set_options(Options).

set_log_level(Level) ->
    ok = erlcass_nif:cass_log_set_level(Level).

set_log_process_receiver(Pid) ->
    ok = erlcass_nif:cass_log_set_callback(Pid).

release() ->
    erlcass_nif:cass_cluster_release().