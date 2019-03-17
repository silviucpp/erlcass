-module(erlcass).

-include("erlcass.hrl").
-include("erlcass_internals.hrl").

-behaviour(gen_server).

-export([
    start_link/0,

    % metrics

    get_metrics/0,

    % queries

    query/1,
    query_async/1,
    query_async/3,
    query_new_statement/1,

    % prepared statements

    add_prepare_statement/2,
    set_paging_size/2,
    async_execute/1,
    async_execute/2,
    async_execute/3,
    async_execute/4,
    async_execute/5,
    execute/1,
    execute/2,
    execute/3,
    async_execute_paged/2,
    async_execute_paged/3,
    execute_paged/2,

    % batch

    batch_execute/3,
    batch_async_execute/3,

    % schema metadata

    get_schema_metadata/0,
    get_schema_metadata/1,
    get_schema_metadata/2,

    % low level methods to deal with statements

    bind_prepared_statement/1,
    bind_prepared_params_by_name/2,
    bind_prepared_params_by_index/2,

    % gen_server callbacks

    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(erlcass_stm, {session, stm}).
-record(state, {session}).

-type query()               :: binary() | {binary(), integer()} | {binary(), list()}.
-type statement_ref()       :: #erlcass_stm{}.
-type bind_type()           :: ?BIND_BY_INDEX | ?BIND_BY_NAME.
-type batch_type()          :: ?CASS_BATCH_TYPE_LOGGED | ?CASS_BATCH_TYPE_UNLOGGED | ?CASS_BATCH_TYPE_COUNTER.
-type tag()                 :: reference().
-type recv_pid()            :: pid() | null.
-type query_result()        :: ok | {ok, list(), list()} | {error, reason()}.
-type paged_query_result()  :: {ok, list(), list(), boolean()} | {error, reason()}.

-spec get_metrics() ->
    {ok, list()} | {error, reason()}.

get_metrics() ->
    call(get_metrics).

-spec get_schema_metadata() ->
    {ok, list()} | {error, reason()}.

get_schema_metadata() ->
    call(get_schema_metadata).

-spec get_schema_metadata(binary()) ->
    {ok, list()} | {error, reason()}.

get_schema_metadata(Keyspace) ->
    call({get_schema_metadata, Keyspace}).

-spec get_schema_metadata(binary(), binary()) ->
    {ok, list()} | {error, reason()}.

get_schema_metadata(Keyspace, Table) ->
    call({get_schema_metadata, Keyspace, Table}).

%non prepared query statements

-spec query_new_statement(query()) ->
    {ok, tag()} | {error, reason()}.

query_new_statement(Query) ->
    erlcass_nif:cass_statement_new(Query).

-spec query_async(query()) ->
    {ok, tag()} | {error, reason()}.

query_async(Q) ->
    query_async(Q, self(), make_ref()).

-spec query_async(query(), recv_pid(), any()) ->
    {ok, tag()} | {error, reason()}.

query_async(Q, ReceiverPid, Tag) ->
    case query_new_statement(Q) of
        {ok, Stm} ->
            case call({execute_normal_statements, get_identifier(ReceiverPid, Q), Stm, ReceiverPid, Tag}) of
                ok ->
                    {ok, Tag};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec query(query()) ->
    query_result().

query(Q) ->
    case query_async(Q) of
        {ok, Tag} ->
            receive_response(Tag);
        Error ->
            Error
    end.

%prepared statements

-spec add_prepare_statement(atom(), query()) ->
    ok | {error, reason()}.

add_prepare_statement(Identifier, Query) ->
    call({add_prepare_statement, Identifier, Query}, ?RESPONSE_TIMEOUT).

-spec bind_prepared_statement(atom()) ->
    {ok, statement_ref()} | {error, reason()}.

bind_prepared_statement(Identifier) ->
    case erlcass_stm_sessions:get(Identifier) of
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

-spec bind_prepared_params_by_name(statement_ref(), list()) ->
    ok | {error, reason()}.

bind_prepared_params_by_name(Stm, Params) ->
    erlcass_nif:cass_statement_bind_parameters(Stm#erlcass_stm.stm, ?BIND_BY_NAME, Params).

-spec bind_prepared_params_by_index(statement_ref(), list()) ->
    ok | {error, reason()}.

bind_prepared_params_by_index(Stm, Params) ->
    erlcass_nif:cass_statement_bind_parameters(Stm#erlcass_stm.stm, ?BIND_BY_INDEX, Params).

-spec async_execute(atom()) ->
    {ok, tag()} | {error, reason()}.

async_execute(Identifier) ->
    case bind_prepared_statement(Identifier) of
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

-spec async_execute(atom() | query(), list()) ->
    {ok, tag()} | {error, reason()}.

async_execute(Identifier, Params) ->
    async_execute(Identifier, ?BIND_BY_INDEX, Params).

-spec async_execute(atom() | binary(), bind_type(), list()) ->
    {ok, tag()} | {error, reason()}.

async_execute(Identifier, BindType, Params) ->
    Tag = make_ref(),
    case async_execute(Identifier, BindType, Params, self(), Tag) of
        ok ->
            {ok, Tag};
        Error ->
            Error
    end.

-spec async_execute(atom() | binary(), bind_type(), list(), any()) ->
    ok | {error, reason()}.

async_execute(Identifier, BindType, Params, Tag) ->
    async_execute(Identifier, BindType, Params, self(), Tag).

-spec async_execute(atom(), bind_type(), list(), recv_pid(), any()) ->
    ok | {error, reason()}.

async_execute(Identifier, BindType, Params, ReceiverPid, Tag) ->
    case bind_prepared_statement(Identifier) of
        {ok, #erlcass_stm{stm = Statement, session = Session}} ->
            case erlcass_nif:cass_statement_bind_parameters(Statement, BindType, Params) of
                ok ->
                    erlcass_nif:cass_session_execute(get_identifier(ReceiverPid, Identifier), Session, Statement, ReceiverPid, Tag);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec execute(atom()) ->
    query_result().

execute(Identifier) ->
    case async_execute(Identifier) of
        {ok, Tag} ->
            receive_response(Tag);
        Error ->
            Error
    end.

-spec execute(atom(), list()) ->
    query_result().

execute(Identifier, Params) ->
    execute(Identifier, ?BIND_BY_INDEX, Params).

-spec execute(atom(), bind_type(), list()) ->
    query_result().

execute(Identifier, BindType, Params) ->
    case async_execute(Identifier, BindType, Params) of
        {ok, Tag} ->
            receive_response(Tag);
        Error ->
            Error
    end.

-spec set_paging_size(statement_ref(), integer()) ->
    ok | {error, reason()}.

set_paging_size(Stm, PageSize) ->
    erlcass_nif:cass_statement_set_paging_size(Stm#erlcass_stm.stm, PageSize).

-spec async_execute_paged(statement_ref(), atom()) ->
    {ok, tag()} | {error, reason()}.

async_execute_paged(Stm, Identifier) ->
    Tag = make_ref(),
    ReceiverPid = self(),
    case erlcass_nif:cass_session_execute_paged(get_identifier(ReceiverPid, Identifier), Stm#erlcass_stm.session, Stm#erlcass_stm.stm, ReceiverPid, Tag) of
        ok ->
            {ok, Tag};
        Error ->
            Error
    end.

-spec async_execute_paged(statement_ref(), atom(), recv_pid()) ->
    {ok, tag()} | {error, reason()}.

async_execute_paged(Stm, Identifier, ReceiverPid) ->
    Tag = make_ref(),
    case erlcass_nif:cass_session_execute_paged(get_identifier(ReceiverPid, Identifier), Stm#erlcass_stm.session, Stm#erlcass_stm.stm, ReceiverPid, Tag) of
        ok ->
            {ok, Tag};
        Error ->
            Error
    end.

-spec execute_paged(statement_ref(), atom()) ->
    paged_query_result().

execute_paged(Stm, Identifier) ->
    case async_execute_paged(Stm, Identifier) of
        {ok, Tag} ->
            receive_paged_response(Tag);
        Error ->
            Error
    end.

-spec batch_async_execute(batch_type(), list(), list()) ->
    {ok, tag()} | {error, reason()}.

batch_async_execute(BatchType, StmList, Options) ->
    call({batch_execute, BatchType, StmList, Options}).

-spec batch_execute(batch_type(), list(), list()) ->
    query_result().

batch_execute(BatchType, StmList, Options) ->
    case batch_async_execute(BatchType, StmList, Options) of
        {ok, Tag} ->
            receive_response(Tag);
        Error ->
            Error
    end.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%internal functions

init([]) ->
    process_flag(trap_exit, true),

    ok = erlcass_stm_sessions:create(),

    case session_create() of
        {ok, SessionRef} ->
            session_prepare_cached_statements(SessionRef),
            {ok, #state{session = SessionRef}};
        Error ->
            {stop, Error, shutdown, #state{}}
    end.

handle_call({execute_normal_statements, Identifier, StmRef, ReceiverPid, Tag}, _From, State) ->
    {reply, erlcass_nif:cass_session_execute(Identifier, State#state.session, StmRef, ReceiverPid, Tag), State};

handle_call({batch_execute, BatchType, StmList, Options}, From, State) ->
    {FromPid, _} = From,
    {reply, erlcass_nif:cass_session_execute_batch(State#state.session, BatchType, filter_stm_list(StmList), Options, FromPid), State};

handle_call({add_prepare_statement, Identifier, Query}, From, State) ->
    case erlcass_stm_cache:find(Identifier) of
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
            erlcass_stm_cache:set(Identifier, Query),
            erlcass_stm_sessions:set(Identifier, Session, StmRef),
            gen_server:reply(From, ok);
        _ ->
            gen_server:reply(From, Result)
    end,
    {noreply, State};

handle_info(Info, State) ->
    ?ERROR_MSG("session ~p received unexpected message: ~p", [self(), Info]),
    {noreply, State}.

terminate(Reason, #state {session = Session}) ->
    Self = self(),
    ?INFO_MSG("closing session ~p with reason: ~p", [Self, Reason]),

    case do_close(Session, Self, ?CONNECT_TIMEOUT) of
        ok ->
            ?INFO_MSG("session ~p closed completed", [Self]);
        Error ->
            ?ERROR_MSG("session ~p closed with error: ~p", [Self, Error])
    end.

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

receive_paged_response(Tag) ->
    receive
        {paged_execute_statement_result, Tag, {ok, Columns, Rows}} ->
            {ok, Columns, Rows, false};
        {paged_execute_statement_result_has_more, Tag, {ok, Columns, Rows}} ->
            {ok, Columns, Rows, true};
        {paged_execute_statement_result, Tag, Error} ->
            Error;
        {paged_execute_statement_result_has_more, Tag, Error} ->
            Error

    after ?RESPONSE_TIMEOUT ->
        {error, timeout}
    end.

do_connect(Session, Pid, Keyspace) ->
    erlcass_nif:cass_session_connect(Session, Pid, Keyspace).

do_connect(Session, Pid) ->
    erlcass_nif:cass_session_connect(Session, Pid).

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

session_create() ->
    case erlcass_nif:cass_session_new() of
        {ok, Session} ->
            Self = self(),
            Keyspace = case erlcass_utils:get_env(keyspace) of
                {ok, Space} -> Space;
                _ -> ""
            end,
            KeyspaceCQL = case erlcass_utils:get_env(keyspace_cql) of
                {ok, CQL} -> CQL;
                _ -> ""
            end,
            Connect = case Keyspace of
              ""       -> do_connect(Session, Self);
              Keyspace -> do_connect(Session, Self, Keyspace)
            end,
            case Connect of
                ok ->
                    case receive_session_connect(Keyspace, Self) of
                        ok -> {ok, Session};
                        {error, missing_keyspace} when KeyspaceCQL =/= "", Keyspace =/= "" ->
                            ?INFO_MSG("Keyspace '~s' is missing, will create using: '~s'", [Keyspace, KeyspaceCQL]),
                            ok = do_connect(Session, Self),
                            case receive_session_connect("", Self) of
                                ok ->
                                    {ok, StmRef} = query_new_statement(KeyspaceCQL),
                                    erlcass_nif:cass_session_execute(nil, Session, StmRef, Self, init_keyspace),
                                    ?INFO_MSG("Creating Keyspace '~s'", [Keyspace]),
                                    ok = receive_response(init_keyspace),
                                    ?INFO_MSG("Keyspace '~s' Created", [Keyspace]),
                                    ok = do_close(Session, Self, 5000),
                                    ?INFO_MSG("Session Closed", []);
                                Error -> Error
                            end,
                            ?INFO_MSG("Reconnecting with Keyspace", []),
                            ok = do_connect(Session, Self, Keyspace),
                            ?INFO_MSG("Waiting for coonection", []),
                            case receive_session_connect(Keyspace, Self) of
                                ok -> {ok, Session};
                                Err -> Err
                            end;
                        Error -> Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

receive_session_connect(Keyspace, Self) ->
    MissingKeyspaceError = list_to_binary(lists:flatten(io_lib:format("Keyspace '~s' does not exist", [Keyspace]))),
    receive
        {session_connected, Self, {error, MissingKeyspaceError}} ->
            {error, missing_keyspace};

        {session_connected, Self, Result} ->
            ?INFO_MSG("session ~p connection complete result: ~p", [Self, Result]),
            ok

    after ?CONNECT_TIMEOUT ->
        ?ERROR_MSG("session ~p connection timeout", [Self]),
        {error, connect_session_timeout}
    end.

session_prepare_cached_statements(SessionRef) ->

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
                                erlcass_stm_sessions:set(Identifier, SessionRef, StmRef);
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
    ok = lists:foreach(FunPrepareStm, erlcass_stm_cache:to_list()).

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

call(Message) ->
    call(Message, 5000).

call(Message, Timeout) ->
    try
        gen_server:call(?MODULE, Message, Timeout)
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

-spec id2bin(Id) -> Bin when
    Id :: atom() | binary() | {atom() | binary(), term()},
    Bin :: binary().
id2bin(Id) when is_atom(Id) ->
    atom_to_binary(Id, latin1);
id2bin(Id) when is_binary(Id) ->
    Id;
id2bin({Id, _Opts}) ->
    id2bin(Id).
