-module(erlcass).
-author("silviu.caragea").

-include("erlcass.hrl").
-include("erlcass_internals.hrl").

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([
    start_link/0,
    get_metrics/0,
    add_prepare_statement/2,
    async_execute/1,
    async_execute/2,
    async_execute/3,
    execute/1,
    execute/2,
    execute/3,
    batch_execute/3,
    batch_async_execute/3,
    create_statement/1,
    create_statement/2,
    bind_prepared_statement/1,
    bind_prepared_params_by_name/2,
    bind_prepared_params_by_index/2,
    async_execute_statement/1,
    execute_statement/1
]).

-define(SERVER, ?MODULE).

-record(erlcass_stm, {session, stm}).
-record(state, {session}).

-spec get_metrics() ->
    {ok, list()} | {error, reason()}.

get_metrics() ->
    gen_server:call(?MODULE, get_metrics).

-spec create_statement(stm(), list()) ->
    {ok, stm_ref()} | {error, reason()}.

create_statement(Query, BindParams) ->
    erlcass_nif:cass_statement_new(Query, BindParams).

-spec create_statement(stm()) ->
    {ok, stm_ref()} | {error, reason()}.

create_statement(Query) ->
    erlcass_nif:cass_statement_new(Query).

-spec add_prepare_statement(atom(), stm()) ->
    ok | {error, reason()}.

add_prepare_statement(Identifier, Query) ->
    gen_server:call(?MODULE, {add_prepare_statement, Identifier, Query}, ?RESPONSE_TIMEOUT).

-spec bind_prepared_statement(atom()) ->
    {ok, #erlcass_stm{}} | {error, reason()}.

bind_prepared_statement(Identifier) ->
    case erlcass_stm_sessions:get(Identifier) of
        undefined ->
            {error, undefined};
        {Session, PrepStatement} ->
            {ok, StmRef} = erlcass_nif:cass_prepared_bind(PrepStatement),
            {ok, #erlcass_stm{session = Session, stm = StmRef}}
    end.

-spec bind_prepared_params_by_name(#erlcass_stm{} | stm_ref() , list()) ->
    ok | {error, reason()}.

bind_prepared_params_by_name(Stm, Params) when is_record(Stm, erlcass_stm) ->
    erlcass_nif:cass_statement_bind_parameters(Stm#erlcass_stm.stm, ?BIND_BY_NAME, Params);
bind_prepared_params_by_name(Stm, Params) ->
    erlcass_nif:cass_statement_bind_parameters(Stm, ?BIND_BY_NAME, Params).

-spec bind_prepared_params_by_index(#erlcass_stm{} | stm_ref(), list()) ->
    ok | {error, reason()}.

bind_prepared_params_by_index(Stm, Params) when is_record(Stm, erlcass_stm) ->
    erlcass_nif:cass_statement_bind_parameters(Stm#erlcass_stm.stm, ?BIND_BY_INDEX, Params);
bind_prepared_params_by_index(Stm, Params) ->
    erlcass_nif:cass_statement_bind_parameters(Stm, ?BIND_BY_INDEX, Params).

-spec async_execute_statement(stm_ref() | #erlcass_stm{}) ->
    {ok, reference()} | {error, reason()}.

async_execute_statement(Stm) when is_record(Stm, erlcass_stm) ->
    Tag = make_ref(),
    Result = erlcass_nif:cass_session_execute(Stm#erlcass_stm.session, Stm#erlcass_stm.stm, self(), Tag),
    {Result, Tag};
async_execute_statement(Stm) ->
    gen_server:call(?MODULE, {execute_normal_statements, Stm}).

-spec execute_statement(stm_ref()) ->
    {ok, list()} | {error, reason()}.

execute_statement(StmRef) ->
    {ok, Tag} = async_execute_statement(StmRef),
    receive_response(Tag).

-spec async_execute(atom() | binary()) ->
    {ok, reference()} | {error, reason()}.

async_execute(Identifier) ->
    case is_atom(Identifier) of
        true ->
            {ok, Statement} = bind_prepared_statement(Identifier);
        _ ->
            {ok, Statement} = create_statement(Identifier)
    end,

    async_execute_statement(Statement).

-spec async_execute(atom() | binary(), list()) ->
    {ok, reference()} | {error, reason()}.

async_execute(Identifier, Params) ->
    async_execute(Identifier, ?BIND_BY_INDEX, Params).

-spec async_execute(atom() | binary(), integer(), list()) ->
    {ok, reference()} | {error, reason()}.

async_execute(Identifier, BindType, Params) ->
    case is_atom(Identifier) of
        true ->
            {ok, Stm} = bind_prepared_statement(Identifier),
            ok = erlcass_nif:cass_statement_bind_parameters(Stm#erlcass_stm.stm, BindType, Params);
        _ ->
            {ok, Stm} = create_statement(Identifier, Params)
    end,

    async_execute_statement(Stm).

-spec execute(atom() | binary()) ->
    {ok, list()} | {error, reason()}.

execute(Identifier) ->
    {ok, Tag} = async_execute(Identifier),
    receive_response(Tag).

-spec execute(atom() | binary(), list()) ->
    {ok, list()} | {error, reason()}.

execute(Identifier, Params) ->
    execute(Identifier, ?BIND_BY_INDEX, Params).

-spec execute(atom() | binary(), integer(), list()) ->
    {ok, list()} | {error, reason()}.

execute(Identifier, BindType, Params) ->
    {ok, Tag} = async_execute(Identifier, BindType, Params),
    receive_response(Tag).

-spec batch_async_execute(integer(), list(), list()) ->
    {ok, reference()} | {error, reason()}.

batch_async_execute(BatchType, StmList, Options) ->
    gen_server:call(?MODULE, {batch_execute, BatchType, StmList, Options}).

-spec batch_execute(integer(), list(), list()) ->
    {ok, list()} | {error, reason()}.

batch_execute(BatchType, StmList, Options) ->
    {ok, Tag} = batch_async_execute(BatchType, StmList, Options),
    receive_response(Tag).

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
        UnexpectedError ->
            {stop, UnexpectedError, shutdown, #state{}}
    end.

handle_call({execute_normal_statements, StmRef}, From, State) ->
    Tag = make_ref(),
    {FromPid, _} = From,
    Result = erlcass_nif:cass_session_execute(State#state.session, StmRef, FromPid, Tag),
    {reply, {Result, Tag}, State};

handle_call({batch_execute, BatchType, StmList, Options}, From, State) ->
    {FromPid, _} = From,
    Result = erlcass_nif:cass_session_execute_batch(State#state.session, BatchType, filter_stm_list(StmList), Options, FromPid),
    {reply, Result, State};

handle_call({add_prepare_statement, Identifier, Query}, From, State) ->
    case erlcass_stm_cache:find(Identifier) of
        false ->
            ok = erlcass_nif:cass_session_prepare(State#state.session, Query, {From, Identifier, Query}),
            {noreply, State};
        true ->
            {reply, {error, already_exist}, State}
    end;

handle_call(get_metrics, _From, State) ->
    {reply, erlcass_nif:cass_session_get_metrics(State#state.session), State}.

handle_cast(Request, State) ->
    ?ERROR_MSG("session ~p received unexpected cast: ~p", [self(), Request]),
    {noreply, State}.

handle_info({prepared_statememt_result, Result, {From, Identifier, Query}}, State) ->

    ?INFO_MSG("session: ~p prepared statement id: ~p result: ~p", [self(), Identifier, Result]),

    case Result of
        {ok, StmRef} ->
            erlcass_stm_cache:set(Identifier, Query),
            erlcass_stm_sessions:set(Identifier, State#state.session, StmRef),
            gen_server:reply(From, ok);
        _ ->
            gen_server:reply(From, Result)
    end,
    {noreply, State};

handle_info(Info, State) ->
    ?ERROR_MSG("session ~p received unexpected message: ~p", [self(), Info]),
    {noreply, State}.

terminate(Reason, State) ->
    Self = self(),
    ?INFO_MSG("closing session ~p with reason: ~p", [Self, Reason]),

    case State#state.session of
        undefined ->
            ok;
        SessionRef ->
            {ok, Tag} = erlcass_nif:cass_session_close(SessionRef),

            receive
                {session_closed, Tag, Result} ->
                    ?INFO_MSG("session ~p closed with result: ~p", [Self, Result])

            after ?RESPONSE_TIMEOUT ->
                ?ERROR_MSG("session ~p closed timeout", [Self])
            end
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

receive_response(Tag) ->
    receive
        {execute_statement_result, Tag, Result} ->
            Result

    after ?RESPONSE_TIMEOUT ->
        timeout
    end.

session_create() ->
    {ok, Session} = erlcass_nif:cass_session_new(),

    case erlcass_utils:get_env(keyspace) of
        {ok, Keyspace} ->
            erlcass_nif:cass_session_connect_keyspace(Session, self(), Keyspace);
        _ ->
            erlcass_nif:cass_session_connect(Session, self())
    end,

    receive
        {session_connected, _Pid} ->
            ?INFO_MSG("session ~p connection completed", [self()]),
            {ok, Session}
    after ?CONNECT_TIMEOUT ->
        ?ERROR_MSG("session ~p connection timeout", [self()]),
        {error, connect_session_timeout}
    end.

session_prepare_cached_statements(SessionRef) ->
    Self = self(),

    FunPrepareStm = fun({Identifier, Query}) ->
        Tag = make_ref(),
        ok = erlcass_nif:cass_session_prepare(SessionRef, Query, Tag),

        receive
            {prepared_statememt_result, Result, Tag} ->

                ?INFO_MSG("session ~p prepared cached statement id: ~p result: ~p", [Self, Identifier, Result]),

                case Result of
                    {ok, StmRef} ->
                        erlcass_stm_sessions:set(Identifier, SessionRef, StmRef);
                    _ ->
                        throw({error, {failed_to_prepare_cached_stm, Identifier, Result}})
                end

        after ?RESPONSE_TIMEOUT ->
            throw({error, {failed_to_prepare_cached_stm, Identifier, timeout}})
        end
    end,
    ok = lists:foreach(FunPrepareStm, erlcass_stm_cache:to_list()).

filter_stm_list(StmList) ->
    filter_stm_list(StmList, []).

filter_stm_list([H|T], Acc) when is_record(H, erlcass_stm) ->
    filter_stm_list(T, [H#erlcass_stm.stm | Acc]);
filter_stm_list([H|T], Acc) ->
    filter_stm_list(T, [H | Acc]);
filter_stm_list([], Acc) ->
    Acc.
