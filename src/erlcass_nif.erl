-module(erlcass_nif).

-include("erlcass_internals.hrl").

-define(NOT_LOADED, not_loaded(?LINE)).

-on_load(load_nif/0).

-export([
    cass_log_set_level/1,
    cass_log_set_callback/1,
    cass_cluster_create/0,
    cass_cluster_release/0,
    cass_cluster_set_options/1,
    cass_session_new/0,
    cass_session_connect/2,
    cass_session_connect/3,
    cass_session_close/2,
    cass_session_prepare/4,
    cass_session_execute/5,
    cass_session_execute_paged/5,
    cass_session_execute_batch/5,
    cass_session_get_metrics/1,
    cass_session_get_schema_metadata/1,
    cass_session_get_schema_metadata/2,
    cass_session_get_schema_metadata/3,
    cass_prepared_bind/1,
    cass_statement_new/1,
    cass_statement_set_paging_size/2,
    cass_statement_bind_parameters/3,
    cass_uuid_gen_new/0,
    cass_uuid_gen_time/0,
    cass_uuid_gen_random/0,
    cass_uuid_gen_from_time/1,
    cass_uuid_min_from_time/1,
    cass_uuid_max_from_time/1,
    cass_uuid_timestamp/1,
    cass_uuid_version/1,
    cass_time_from_epoch/1,
    cass_date_from_epoch/1,
    cass_date_time_to_epoch/2
]).

%% nif functions

load_nif() ->
    SoName = get_priv_path(?MODULE),
    ?INFO_MSG("loading library: ~p ~n", [SoName]),
    ok = erlang:load_nif(SoName, 0).

get_priv_path(File) ->
    case code:priv_dir(erlcass) of
        {error, bad_name} ->
            Ebin = filename:dirname(code:which(?MODULE)),
            filename:join([filename:dirname(Ebin), "priv", File]);
        Dir ->
            filename:join(Dir, File)
    end.

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).

cass_log_set_level(_Level) ->
    ?NOT_LOADED.

cass_log_set_callback(_LogPid) ->
    ?NOT_LOADED.

cass_cluster_create() ->
    ?NOT_LOADED.

cass_cluster_release() ->
    ?NOT_LOADED.

cass_cluster_set_options(_OptionList) ->
    ?NOT_LOADED.

cass_session_new() ->
    ?NOT_LOADED.

cass_session_connect(_SessionRef, _FromPid) ->
    ?NOT_LOADED.

cass_session_connect(_SessionRef, _FromPid, _Keyspace) ->
    ?NOT_LOADED.

cass_session_close(_SessionRef, _Pid) ->
    ?NOT_LOADED.

cass_session_prepare(_SessionRef, _Pid, _Query, _Info) ->
    ?NOT_LOADED.

cass_session_execute(_Identifier, _SessionRef, _StatementRef, _FromPid, _Tag) ->
    ?NOT_LOADED.

cass_session_execute_paged(_Identifier, _SessionRef, _StatementRef, _FromPid, _Tag) ->
    ?NOT_LOADED.

cass_session_execute_batch(_SessionRef, _BatchType, _StmList, _Options, _Pid) ->
    ?NOT_LOADED.

cass_session_get_metrics(_SessionRef) ->
    ?NOT_LOADED.

cass_session_get_schema_metadata(_SessionRef) ->
    ?NOT_LOADED.

cass_session_get_schema_metadata(_SessionRef, _KeySpace) ->
    ?NOT_LOADED.

cass_session_get_schema_metadata(_SessionRef, _KeySpace, _Table) ->
    ?NOT_LOADED.

cass_prepared_bind(_PrepStatementRef) ->
    ?NOT_LOADED.

cass_statement_new(_Query) ->
    ?NOT_LOADED.

cass_statement_set_paging_size(_StatementRef, _PageSize)->
    ?NOT_LOADED.

cass_statement_bind_parameters(_StatementRef, _BindType, _Args)->
    ?NOT_LOADED.

cass_uuid_gen_new() ->
    ?NOT_LOADED.

cass_uuid_gen_time() ->
    ?NOT_LOADED.

cass_uuid_gen_random() ->
    ?NOT_LOADED.

cass_uuid_gen_from_time(_Ts) ->
    ?NOT_LOADED.

cass_uuid_min_from_time(_Ts) ->
    ?NOT_LOADED.

cass_uuid_max_from_time(_Ts) ->
    ?NOT_LOADED.

cass_uuid_timestamp(_Uuid) ->
    ?NOT_LOADED.

cass_uuid_version(_Uuid) ->
    ?NOT_LOADED.

cass_time_from_epoch(_EpochSecs) ->
    ?NOT_LOADED.

cass_date_from_epoch(_EpochSecs) ->
    ?NOT_LOADED.

cass_date_time_to_epoch(_Date, _Time) ->
    ?NOT_LOADED.