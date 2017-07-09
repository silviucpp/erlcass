-module(erlcass).

-include("erlcass.hrl").
-include("erlcass_internals.hrl").

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([
    start_link/0,
    get_metrics/0,

    get_schema_metadata/0,
    get_schema_metadata/1,
    get_schema_metadata/2,

    %queries

    query/1,
    query_async/1,
    query_new_statement/1,

    %prepared statements

    add_prepare_statement/2,
    async_execute/1,
    async_execute/2,
    async_execute/3,
    async_execute/4,
    async_execute/5,
    execute/1,
    execute/2,
    execute/3,

    %batch

    batch_execute/3,
    batch_async_execute/3,

    %low level methods to deal with statements

    bind_prepared_statement/1,
    bind_prepared_params_by_name/2,
    bind_prepared_params_by_index/2
]).

-define(SERVER, ?MODULE).

-record(state, {session}).

-spec get_metrics() -> {ok, list()} | {error, reason()}.

get_metrics() ->
    call(get_metrics).


get_schema_metadata() ->
    call(get_schema_metadata).

get_schema_metadata(Keyspace) ->
    call({get_schema_metadata, Keyspace}).

get_schema_metadata(Keyspace, Table) ->
    call({get_schema_metadata, Keyspace, Table}).


%non prepared query statements

-spec query_new_statement(query()) -> {ok, reference()} | {error, reason()}.

query_new_statement(Query) ->
    erlcass_nif:cass_statement_new(Query).

-spec query_async(query()) -> {ok, tag()} | {error, reason()}.

query_async(Q) ->
    case query_new_statement(Q) of
        {ok, Stm} ->
            Tag = make_ref(),
            case call({execute_normal_statements, Stm, self(), Tag}) of
                ok ->
                    {ok, Tag};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec query(query()) -> {ok, list()} | {error, reason()}.

query(Q) ->
    case query_async(Q) of
        {ok, Tag} ->
            receive_response(Tag);
        Error ->
            Error
    end.

%prepared statements

-spec add_prepare_statement(atom(), query()) -> ok | {error, reason()}.

add_prepare_statement(Identifier, Query) ->
    call({add_prepare_statement, Identifier, Query}, ?RESPONSE_TIMEOUT).

-spec bind_prepared_statement(atom()) -> {ok, statement_ref()} | {error, reason()}.

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

-spec bind_prepared_params_by_name(statement_ref(), list()) -> ok | {error, reason()}.

bind_prepared_params_by_name(Stm, Params) ->
    erlcass_nif:cass_statement_bind_parameters(Stm#erlcass_stm.stm, ?BIND_BY_NAME, Params).

-spec bind_prepared_params_by_index(statement_ref(), list()) -> ok | {error, reason()}.

bind_prepared_params_by_index(Stm, Params) ->
    erlcass_nif:cass_statement_bind_parameters(Stm#erlcass_stm.stm, ?BIND_BY_INDEX, Params).

-spec async_execute(atom()) -> {ok, tag()} | {error, reason()}.

async_execute(Identifier) ->
    case bind_prepared_statement(Identifier) of
        {ok, Stm} ->
            Tag = make_ref(),
            case erlcass_nif:cass_session_execute(Stm#erlcass_stm.session, Stm#erlcass_stm.stm, self(), Tag) of
                ok ->
                    {ok, Tag};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec async_execute(atom() | query(), list()) -> {ok, tag()} | {error, reason()}.

async_execute(Identifier, Params) ->
    async_execute(Identifier, ?BIND_BY_INDEX, Params).

-spec async_execute(atom() | binary(), bind_type(), list()) -> {ok, tag()} | {error, reason()}.

async_execute(Identifier, BindType, Params) ->
    Tag = make_ref(),
    case async_execute(Identifier, BindType, Params, self(), Tag) of
        ok ->
            {ok, Tag};
        Error ->
            Error
    end.

-spec async_execute(atom() | binary(), bind_type(), list(), any()) -> ok | {error, reason()}.

async_execute(Identifier, BindType, Params, Tag) ->
    async_execute(Identifier, BindType, Params, self(), Tag).

-spec async_execute(atom(), bind_type(), list(), pid() | null, any()) -> ok | {error, reason()}.

async_execute(Identifier, BindType, Params, ReceiverPid, Tag) ->
    case bind_prepared_statement(Identifier) of
        {ok, Stm} ->
            case erlcass_nif:cass_statement_bind_parameters(Stm#erlcass_stm.stm, BindType, Params) of
                ok ->
                    erlcass_nif:cass_session_execute(Stm#erlcass_stm.session, Stm#erlcass_stm.stm, ReceiverPid, Tag);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec execute(atom()) -> {ok, list()} | {error, reason()}.

execute(Identifier) ->
    case async_execute(Identifier) of
        {ok, Tag} ->
            receive_response(Tag);
        Error ->
            Error
    end.

-spec execute(atom(), list()) -> {ok, list()} | {error, reason()}.

execute(Identifier, Params) ->
    execute(Identifier, ?BIND_BY_INDEX, Params).

-spec execute(atom(), bind_type(), list()) -> {ok, list()} | {error, reason()}.

execute(Identifier, BindType, Params) ->
    case async_execute(Identifier, BindType, Params) of
        {ok, Tag} ->
            receive_response(Tag);
        Error ->
            Error
    end.

-spec batch_async_execute(batch_type(), list(), list()) -> {ok, tag()} | {error, reason()}.

batch_async_execute(BatchType, StmList, Options) ->
    call({batch_execute, BatchType, StmList, Options}).

-spec batch_execute(batch_type(), list(), list()) -> {ok, list()} | {error, reason()}.

batch_execute(BatchType, StmList, Options) ->
    case batch_async_execute(BatchType, StmList, Options) of
        {ok, Tag} ->
            receive_response(Tag);
        Error ->
            Error
    end.

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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

handle_call({execute_normal_statements, StmRef, ReceiverPid, Tag}, _From, State) ->
    {reply, erlcass_nif:cass_session_execute(State#state.session, StmRef, ReceiverPid, Tag), State};

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

do_connect(Session, Pid) ->
    case erlcass_utils:get_env(keyspace) of
        {ok, Keyspace} ->
            erlcass_nif:cass_session_connect(Session, Pid, Keyspace);
        _ ->
            erlcass_nif:cass_session_connect(Session, Pid)
    end.

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
            case do_connect(Session, Self) of
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
