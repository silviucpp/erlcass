-module(erlcass).

-include("erlcass.hrl").
-include("erlcass_internals.hrl").

-behaviour(gen_server).

-export([
    start_link/1,

    % metrics

    get_metrics/1,

    % queries

    query/2,
    query_async/2,
    query_async/4,
    query_new_statement/2,

    % prepared statements

    add_prepare_statement/3,
    async_execute/2,
    async_execute/3,
    async_execute/4,
    async_execute/5,
    async_execute/6,
    execute/2,
    execute/3,
    execute/4,

    % batch

    batch_execute/4,
    batch_async_execute/4,

    % schema metadata

    get_schema_metadata/1,
    get_schema_metadata/2,
    get_schema_metadata/3,

    % low level methods to deal with statements

    bind_prepared_statement/2,
    bind_prepared_params_by_name/3,
    bind_prepared_params_by_index/3,

    % gen_server callbacks

    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(erlcass_stm, {session, stm}).
-record(state, {cluster, session, config, stm_cache_table, stm_session_table}).

-type query()           :: binary() | {binary(), integer()} | {binary(), list()}.
-type statement_ref()   :: #erlcass_stm{}.
-type bind_type()       :: ?BIND_BY_INDEX | ?BIND_BY_NAME.
-type batch_type()      :: ?CASS_BATCH_TYPE_LOGGED | ?CASS_BATCH_TYPE_UNLOGGED | ?CASS_BATCH_TYPE_COUNTER.
-type tag()             :: reference().
-type recv_pid()        :: pid() | null.
-type query_result()    :: ok | {ok, list(), list()} | {error, reason()}.

-spec get_metrics(pid()) ->
    {ok, list()} | {error, reason()}.

get_metrics(Pid) ->
    call(Pid, get_metrics).

-spec get_schema_metadata(pid()) ->
    {ok, list()} | {error, reason()}.

get_schema_metadata(Pid) ->
    call(Pid, get_schema_metadata).

-spec get_schema_metadata(pid(), binary()) ->
    {ok, list()} | {error, reason()}.

get_schema_metadata(Pid, Keyspace) ->
    call(Pid, {get_schema_metadata, Keyspace}).

-spec get_schema_metadata(pid(), binary(), binary()) ->
    {ok, list()} | {error, reason()}.

get_schema_metadata(Pid, Keyspace, Table) ->
    call(Pid, {get_schema_metadata, Keyspace, Table}).

%non prepared query statements

-spec query_new_statement(pid(), query()) ->
    {ok, tag()} | {error, reason()}.

query_new_statement(_Pid, Query) ->
    erlcass_nif:cass_statement_new(Query).

-spec query_async(pid(), query()) ->
    {ok, tag()} | {error, reason()}.

query_async(Pid, Q) ->
    query_async(Pid, Q, self(), make_ref()).

-spec query_async(pid(), query(), recv_pid(), any()) ->
    {ok, tag()} | {error, reason()}.

query_async(Pid, Q, ReceiverPid, Tag) ->
    case query_new_statement(Pid, Q) of
        {ok, Stm} ->
            case call(Pid, {execute_normal_statements, get_identifier(ReceiverPid, Q), Stm, ReceiverPid, Tag}) of
                ok ->
                    {ok, Tag};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec query(pid(), query()) ->
    query_result().

query(Pid, Q) ->
    case query_async(Pid, Q) of
        {ok, Tag} ->
            receive_response(Tag);
        Error ->
            Error
    end.

%prepared statements

-spec add_prepare_statement(pid(), atom(), query()) ->
    ok | {error, reason()}.

add_prepare_statement(Pid, Identifier, Query) ->
    call(Pid, {add_prepare_statement, Identifier, Query}, ?RESPONSE_TIMEOUT).

-spec bind_prepared_statement(pid(), atom()) ->
    {ok, statement_ref()} | {error, reason()}.

bind_prepared_statement(Pid, Identifier) when is_atom(Pid) ->
    StmSessionTable = erlcass_stm_sessions:get_existing_table_name(Pid),

    case erlcass_stm_sessions:get(StmSessionTable, Identifier) of
        {Session, PrepStatement} ->
            case erlcass_nif:cass_prepared_bind(PrepStatement) of
                {ok, StmRef} ->
                    {ok, #erlcass_stm{session = Session, stm = StmRef}};
                Error ->
                    Error
            end;
        undefined ->
            {error, undefined};
        Error ->
            Error
    end.

-spec bind_prepared_params_by_name(pid(), statement_ref(), list()) ->
    ok | {error, reason()}.

bind_prepared_params_by_name(_Pid, Stm, Params) ->
    erlcass_nif:cass_statement_bind_parameters(Stm#erlcass_stm.stm, ?BIND_BY_NAME, Params).

-spec bind_prepared_params_by_index(pid(), statement_ref(), list()) ->
    ok | {error, reason()}.

bind_prepared_params_by_index(_Pid, Stm, Params) ->
    erlcass_nif:cass_statement_bind_parameters(Stm#erlcass_stm.stm, ?BIND_BY_INDEX, Params).

-spec async_execute(pid(), atom()) ->
    {ok, tag()} | {error, reason()}.

async_execute(Pid, Identifier) ->
    case bind_prepared_statement(Pid, Identifier) of
        {ok, Stm} ->
            Tag = make_ref(),
            ReceiverPid = self(),
            case erlcass_nif:cass_session_execute(get_identifier(ReceiverPid, Identifier), Stm#erlcass_stm.session, Stm#erlcass_stm.stm, ReceiverPid, Tag) of
                ok ->
                    {ok, Tag};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec async_execute(pid(), atom() | query(), list()) ->
    {ok, tag()} | {error, reason()}.

async_execute(Pid, Identifier, Params) ->
    async_execute(Pid, Identifier, ?BIND_BY_INDEX, Params).

-spec async_execute(pid(), atom() | binary(), bind_type(), list()) ->
    {ok, tag()} | {error, reason()}.

async_execute(Pid, Identifier, BindType, Params) ->
    Tag = make_ref(),
    case async_execute(Pid, Identifier, BindType, Params, self(), Tag) of
        ok ->
            {ok, Tag};
        Error ->
            Error
    end.

-spec async_execute(pid(), atom() | binary(), bind_type(), list(), any()) ->
    ok | {error, reason()}.

async_execute(Pid, Identifier, BindType, Params, Tag) ->
    async_execute(Pid, Identifier, BindType, Params, self(), Tag).

-spec async_execute(pid(), atom(), bind_type(), list(), recv_pid(), any()) ->
    ok | {error, reason()}.

async_execute(Pid, Identifier, BindType, Params, ReceiverPid, Tag) ->
    case bind_prepared_statement(Pid, Identifier) of
        {ok, Stm} ->
            case erlcass_nif:cass_statement_bind_parameters(Stm#erlcass_stm.stm, BindType, Params) of
                ok ->
                    erlcass_nif:cass_session_execute(get_identifier(ReceiverPid, Identifier), Stm#erlcass_stm.session, Stm#erlcass_stm.stm, ReceiverPid, Tag);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec execute(pid(), atom()) ->
    query_result().

execute(Pid, Identifier) ->
    case async_execute(Pid, Identifier) of
        {ok, Tag} ->
            receive_response(Tag);
        Error ->
            Error
    end.

-spec execute(pid(), atom(), list()) ->
    query_result().

execute(Pid, Identifier, Params) ->
    execute(Pid, Identifier, ?BIND_BY_INDEX, Params).

-spec execute(pid(), atom(), bind_type(), list()) ->
    query_result().

execute(Pid, Identifier, BindType, Params) ->
    case async_execute(Pid, Identifier, BindType, Params) of
        {ok, Tag} ->
            receive_response(Tag);
        Error ->
            Error
    end.

-spec batch_async_execute(pid(), batch_type(), list(), list()) ->
    {ok, tag()} | {error, reason()}.

batch_async_execute(Pid, BatchType, StmList, Options) ->
    call(Pid, {batch_execute, BatchType, StmList, Options}).

-spec batch_execute(pid(), batch_type(), list(), list()) ->
    query_result().

batch_execute(Pid, BatchType, StmList, Options) ->
    case batch_async_execute(Pid, BatchType, StmList, Options) of
        {ok, Tag} ->
            receive_response(Tag);
        Error ->
            Error
    end.

start_link({Name, Config}) ->
    gen_server:start_link({local, Name}, ?MODULE, {Name, Config}, []).

%internal functions

init({Name, Config}) ->
    process_flag(trap_exit, true),

    {ok, Cluster} = erlcass_cluster:create(),

    ClusterOptions = erlcass_utils:except(keyspace, Config),
    erlcass_cluster:set_options(Cluster, ClusterOptions),

    StmCacheTable = erlcass_stm_cache:get_table_name(Name),

    StmSessionTable = erlcass_stm_sessions:get_table_name(Name),
    ok = erlcass_stm_sessions:create(StmSessionTable),

    case session_create(Cluster, Config) of
        {ok, SessionRef} ->
            session_prepare_cached_statements(SessionRef, StmSessionTable, StmCacheTable),
            Response = {ok, #state{session = SessionRef,
                        cluster = Cluster,
                        config = Config,
                        stm_cache_table = StmCacheTable,
                        stm_session_table = StmSessionTable}},
            Response;
        Error ->
            {stop, Error, shutdown, #state{cluster = Cluster, config = Config}}
    end.

handle_call({execute_normal_statements, Identifier, StmRef, ReceiverPid, Tag}, _From, State) ->
    {reply, erlcass_nif:cass_session_execute(Identifier, State#state.session, StmRef, ReceiverPid, Tag), State};

handle_call({batch_execute, BatchType, StmList, Options}, From, State) ->
    {FromPid, _} = From,
    {reply, erlcass_nif:cass_session_execute_batch(State#state.session, BatchType, filter_stm_list(StmList), Options, FromPid), State};

handle_call({add_prepare_statement, Identifier, Query}, From, State) ->
    case erlcass_stm_cache:find(State#state.stm_cache_table, Identifier) of
        false ->
            ok = erlcass_nif:cass_session_prepare(State#state.session, self(), Query, {From, Identifier, Query}),
            {noreply, State};
        _ ->
            {reply, {error, already_exist}, State}
    end;

handle_call(get_schema_metadata, _From, State) ->
    {reply, erlcass_nif:cass_session_get_schema_metadata(State#state.session), State};

handle_call({get_schema_metadata, Keyspace}, _From, State) ->
    {reply, erlcass_nif:cass_session_get_schema_metadata(State#state.session, Keyspace), State};

handle_call({get_schema_metadata, Keyspace, Table}, _From, State) ->
    {reply, erlcass_nif:cass_session_get_schema_metadata(State#state.session, Keyspace, Table), State};

handle_call(get_metrics, _From, #state{session = Session} = State) ->
    {reply, erlcass_nif:cass_session_get_metrics(Session), State}.

handle_cast(Request, State) ->
    ?ERROR_MSG("session ~p received unexpected cast: ~p", [self(), Request]),
    {noreply, State}.

handle_info({prepared_statement_result, Result, {From, Identifier, Query}}, #state{session = Session} = State) ->

    ?INFO_MSG("session: ~p prepared statement id: ~p result: ~p", [self(), Identifier, Result]),

    case Result of
        {ok, StmRef} ->
            erlcass_stm_cache:set(State#state.stm_cache_table, Identifier, Query),
            erlcass_stm_sessions:set(State#state.stm_session_table, Identifier, Session, StmRef),
            gen_server:reply(From, ok);
        _ ->
            gen_server:reply(From, Result)
    end,
    {noreply, State};

handle_info(Info, State) ->
    ?ERROR_MSG("session ~p received unexpected message: ~p", [self(), Info]),
    {noreply, State}.

terminate(Reason, #state {session = Session, cluster = Cluster}) ->
    Self = self(),
    ?INFO_MSG("closing session ~p with reason: ~p", [Self, Reason]),

    case do_close(Session, Self, ?CONNECT_TIMEOUT) of
        ok ->
            ?INFO_MSG("session ~p closed completed", [Self]);
        Error ->
            ?ERROR_MSG("session ~p closed with error: ~p", [Self, Error])
    end,
    ok = erlcass_cluster:release(Cluster).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% internals

receive_response(Tag) ->
    receive
        {execute_statement_result, Tag, Result} ->
            Result

    after ?RESPONSE_TIMEOUT ->
        {error, timeout}
    end.

do_connect(Cluster, Session, Pid, {ok, Keyspace}) ->
    erlcass_nif:cass_session_connect(Cluster, Session, Pid, Keyspace);

do_connect(Cluster, Session, Pid, _Keyspace) ->
    erlcass_nif:cass_session_connect(Cluster, Session, Pid).

do_close(undefined, _Pid, _Timeout) ->
    ok;
do_close(Session, Pid, Timeout) ->
    case erlcass_nif:cass_session_close(Session, Pid) of
        ok ->
            receive
                {session_closed, Pid, Result} ->
                    Result
            after Timeout ->
                {error, timeout}
            end
    end.

session_create(Cluster, Config) ->
    case erlcass_nif:cass_session_new() of
        {ok, Session} ->
            Self = self(),
            Keyspace = erlcass_utils:lookup(keyspace, Config),
            case do_connect(Cluster, Session, Self, Keyspace) of
                ok ->
                    receive
                        {session_connected, Self, Result} ->
                            ?INFO_MSG("session ~p connection complete result: ~p", [Self, Result]),
                            {ok, Session}

                    after ?CONNECT_TIMEOUT ->
                        ?ERROR_MSG("session ~p connection timeout", [Self]),
                        {error, connect_session_timeout}
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

session_prepare_cached_statements(SessionRef, StmSessionTable, StmCacheTable) ->

    FunPrepareStm = fun({Identifier, Query}) ->
        Tag = make_ref(),
        Self = self(),
        case erlcass_nif:cass_session_prepare(SessionRef, Self, Query, Tag) of
            ok ->
                receive
                    {prepared_statement_result, Result, Tag} ->

                        ?INFO_MSG("session ~p prepared cached statement id: ~p result: ~p", [Self, Identifier, Result]),

                        case Result of
                            {ok, StmRef} ->
                                erlcass_stm_sessions:set(StmSessionTable, Identifier, SessionRef, StmRef);
                            _ ->
                                throw({error, {failed_to_prepare_cached_stm, Identifier, Result}})
                        end

                after ?RESPONSE_TIMEOUT ->
                    throw({error, {failed_to_prepare_cached_stm, Identifier, timeout}})
                end;
            Error ->
                throw(Error)
        end
    end,
    ok = lists:foreach(FunPrepareStm, erlcass_stm_cache:to_list(StmCacheTable)).

filter_stm_list(StmList) ->
    filter_stm_list(StmList, []).

filter_stm_list([H|T], Acc) ->
    case is_record(H, erlcass_stm) of
        true ->
            filter_stm_list(T, [H#erlcass_stm.stm | Acc]);
        _ ->
            filter_stm_list(T, [H | Acc])
    end;
filter_stm_list([], Acc) ->
    Acc.

call(Pid, Message) ->
    call(Pid, Message, 5000).

call(Pid, Message, Timeout) ->
    try
        gen_server:call(Pid, Message, Timeout)
    catch
        exit:{noproc, _} ->
            {error, erlcass_not_started};
        _: Exception ->
            {error, Exception}
    end.

get_identifier(null, Identifier) ->
    id2bin(Identifier);
get_identifier(_, _Identifier) ->
    null.

id2bin(Id) when is_atom(Id) ->
    atom_to_binary(Id, utf8);
id2bin(Id) when is_binary(Id) ->
    Id;
id2bin({Id, _Opts}) ->
    id2bin(Id);
id2bin(Other) ->
    erlang:term_to_binary(Other).
