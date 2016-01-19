-module(erlcass_nif).
-author("silviu.caragea").

-define(NOT_LOADED, not_loaded(?LINE)).

-on_load(load_nif/0).

-export([
    cass_cluster_create/0,
    cass_cluster_release/0,
    cass_log_set_level_and_callback/2,
    cass_cluster_set_options/1,
    cass_session_new/0,
    cass_session_connect/2,
    cass_session_connect_keyspace/3,
    cass_session_close/1,
    cass_session_prepare/3,
    cass_prepared_bind/1,
    cass_statement_new/2,
    cass_statement_new/1,
    cass_statement_bind_parameters/3,
    cass_session_execute/4,
    cass_session_execute_batch/5,
    cass_session_get_metrics/1,
    cass_uuid_gen_new/0,
    cass_uuid_gen_time/1,
    cass_uuid_gen_random/1,
    cass_uuid_gen_from_time/2,
    cass_uuid_min_from_time/1,
    cass_uuid_max_from_time/1,
    cass_uuid_timestamp/1,
    cass_uuid_version/1,
    cass_date_from_epoch/1,
    cass_time_from_epoch/1,
    cass_date_time_to_epoch/2
]).

%% nif functions

load_nif() ->
    SoName = get_nif_library_path(),
    io:format(<<"Loading library: ~p ~n">>, [SoName]),
    ok = erlang:load_nif(SoName, 0).

get_nif_library_path() ->
    case code:priv_dir(erlcass) of
        {error, bad_name} ->
            case filelib:is_dir(filename:join(["..", priv])) of
                true ->
                    filename:join(["..", priv, ?MODULE]);
                false ->
                    filename:join([priv, ?MODULE])
            end;
        Dir ->
            filename:join(Dir, ?MODULE)
    end.

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).

cass_cluster_create() ->
    ?NOT_LOADED.

cass_cluster_release() ->
    ?NOT_LOADED.

cass_log_set_level_and_callback(_Level, _LogPid) ->
    ?NOT_LOADED.

cass_cluster_set_options(_OptionList) ->
    ?NOT_LOADED.

cass_session_new() ->
    ?NOT_LOADED.

cass_session_connect(_SessionRef, _FromPid) ->
    ?NOT_LOADED.

cass_session_connect_keyspace(_SessionRef, _FromPid, _Keyspace) ->
    ?NOT_LOADED.

cass_session_close(_SessionRef) ->
    ?NOT_LOADED.

cass_session_prepare(_SessionRef, _Query, _Info) ->
    ?NOT_LOADED.

cass_prepared_bind(_PrepStatementRef) ->
    ?NOT_LOADED.

cass_statement_new(_Query, _Params) ->
    ?NOT_LOADED.

cass_statement_new(_Query) ->
    ?NOT_LOADED.

cass_statement_bind_parameters(_StatementRef, _BindType, _Args)->
    ?NOT_LOADED.

cass_session_execute(_SessionRef, _StatementRef, _FromPid, _Tag) ->
    ?NOT_LOADED.

cass_session_execute_batch(_SessionRef, _BatchType, _StmList, _Options, _Pid) ->
    ?NOT_LOADED.

cass_session_get_metrics(_SessionRef) ->
    ?NOT_LOADED.

cass_uuid_gen_new() ->
    ?NOT_LOADED.

cass_uuid_gen_time(_Generator) ->
    ?NOT_LOADED.

cass_uuid_gen_random(_Generator) ->
    ?NOT_LOADED.

cass_uuid_gen_from_time(_Generator, _Ts) ->
    ?NOT_LOADED.

cass_uuid_min_from_time(_Ts) ->
    ?NOT_LOADED.

cass_uuid_max_from_time(_Ts) ->
    ?NOT_LOADED.

cass_uuid_timestamp(_Uuid) ->
    ?NOT_LOADED.

cass_uuid_version(_Uuid) ->
    ?NOT_LOADED.

cass_date_from_epoch(_EpochSecs) ->
    ?NOT_LOADED.

cass_time_from_epoch(_EpochSecs) ->
    ?NOT_LOADED.

cass_date_time_to_epoch(_Date, _Time) ->
    ?NOT_LOADED.