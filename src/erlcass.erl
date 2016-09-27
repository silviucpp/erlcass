-module(erlcass).
-author("silviu.caragea").

-include("erlcass.hrl").

-define(TIMEOUT, 20000).
-define(CONNECT_TIMEOUT, 5000).

-behaviour(gen_server).

-record(erlcass_stm, {session, stm}).

-export([start_link/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([
    set_cluster_options/1,
    create_session/1,
    set_log_function/1,
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

-record(state, {session, connected}).

-spec(set_cluster_options(OptionList :: list()) ->
    ok | badarg | {error, Reason :: binary()}).

set_cluster_options(Options) ->
    gen_server:call(?MODULE, {set_cluster_options, Options}).

-spec(create_session(Args :: list()) ->
    ok | badarg | {error, Reason :: binary()}).

create_session(Args) ->
    gen_server:call(?MODULE, {create_session, Args}, ?CONNECT_TIMEOUT).

-spec(set_log_function(Func :: term()) ->
    ok | badarg | {error, Reason :: binary()}).

set_log_function(Func) ->
    erlcass_log:update_function(Func).

-spec(get_metrics() ->
    {ok, MetricsList :: list()} | badarg | {error, Reason :: binary()}).

get_metrics() ->
    gen_server:call(?MODULE, get_metrics).

-spec(create_statement(Query :: binary() | {binary(), integer()}, BindParams :: list()) ->
    {ok, StatementRef :: reference()} | badarg | {error, Reason :: binary()}).

create_statement(Query, BindParams) ->
    erlcass_nif:cass_statement_new(Query, BindParams).

-spec(create_statement(Query :: binary() | {binary(), integer()}) ->
    {ok, StatementRef :: reference()} | badarg | {error, Reason :: binary()}).

create_statement(Query) ->
    erlcass_nif:cass_statement_new(Query).

-spec(add_prepare_statement(Identifier :: atom(), Query :: binary() | {binary(), integer()}) ->
    ok | already_exist | badarg | {error, Reason :: binary()}).

add_prepare_statement(Identifier, Query) ->
    gen_server:call(?MODULE, {add_prepare_statement, Identifier, Query}, ?TIMEOUT).

-spec(bind_prepared_statement(Identifier :: atom()) ->
    {ok, Statement :: #erlcass_stm{}} | badarg | {error, Reason :: binary()}).

bind_prepared_statement(Identifier) ->
    case erlcass_prep_utils:get(Identifier) of
        undefined ->
            {error, undefined};
        {Session, PrepStatement} ->
            {ok, StatementRef} = erlcass_nif:cass_prepared_bind(PrepStatement),
            {ok, #erlcass_stm{session = Session, stm = StatementRef}}
    end.

-spec(bind_prepared_params_by_name(Stm :: #erlcass_stm{} | reference() , Params :: list()) ->
    ok | badarg | {error, Reason :: binary()}).

bind_prepared_params_by_name(Stm, Params) when is_record(Stm, erlcass_stm) ->
    erlcass_nif:cass_statement_bind_parameters(Stm#erlcass_stm.stm, ?BIND_BY_NAME, Params);

bind_prepared_params_by_name(Stm, Params) ->
    erlcass_nif:cass_statement_bind_parameters(Stm, ?BIND_BY_NAME, Params).

-spec(bind_prepared_params_by_index(Stm :: #erlcass_stm{} | reference(), Params :: list()) ->
    ok | badarg | {error, Reason :: binary()}).

bind_prepared_params_by_index(Stm, Params) when is_record(Stm, erlcass_stm) ->
    erlcass_nif:cass_statement_bind_parameters(Stm#erlcass_stm.stm, ?BIND_BY_INDEX, Params);

bind_prepared_params_by_index(Stm, Params) ->
    erlcass_nif:cass_statement_bind_parameters(Stm, ?BIND_BY_INDEX, Params).

-spec(async_execute_statement(Stm :: reference() | #erlcass_stm{}) ->
    {ok, Tag :: reference()} | badarg | {error, Reason :: binary()}).

async_execute_statement(Stm) when is_record(Stm, erlcass_stm) ->
    Tag = make_ref(),
    Result = erlcass_nif:cass_session_execute(Stm#erlcass_stm.session, Stm#erlcass_stm.stm, self(), Tag),
    {Result, Tag};

async_execute_statement(Stm) ->
    gen_server:call(?MODULE, {execute_normal_statements, Stm}).

-spec(execute_statement(StatementRef :: reference()) ->
    {ok, Result :: list()} | badarg | {error, Reason :: binary()}).

execute_statement(StatementRef) ->
    {ok, Tag} = async_execute_statement(StatementRef),
    receive_response(Tag).


-spec(async_execute(Identifier :: atom() | binary()) ->
    {ok, Tag :: reference()} | badarg | {error, Reason :: binary()}).

async_execute(Identifier) ->
    if
        is_atom(Identifier) ->
            {ok, Statement} = bind_prepared_statement(Identifier);
        true ->
            {ok, Statement} = create_statement(Identifier)
    end,

    async_execute_statement(Statement).

-spec(async_execute(Identifier :: atom() | binary(), Params :: list()) ->
    {ok, Tag :: reference()} | badarg | {error, Reason :: binary()}).

async_execute(Identifier, Params) ->
    async_execute(Identifier, ?BIND_BY_INDEX, Params).

-spec(async_execute(Identifier :: atom() | binary(), BindType :: integer(), Params :: list()) ->
    {ok, Tag :: reference()} | badarg | {error, Reason :: binary()}).

async_execute(Identifier, BindType, Params) ->
    if
        is_atom(Identifier) ->
            {ok, Stm} = bind_prepared_statement(Identifier),
            ok = erlcass_nif:cass_statement_bind_parameters(Stm#erlcass_stm.stm, BindType, Params);

        true ->
            {ok, Stm} = create_statement(Identifier, Params)
    end,

    async_execute_statement(Stm).

-spec(execute(Identifier :: atom() | binary()) ->
    {ok, Result :: list()} | badarg | {error, Reason :: binary()}).

execute(Identifier) ->
    {ok, Tag} = async_execute(Identifier),
    receive_response(Tag).

-spec(execute(Identifier :: atom() | binary(), Params :: list()) ->
    {ok, Result :: list()} | badarg | {error, Reason :: binary()}).

execute(Identifier, Params) ->
    execute(Identifier, ?BIND_BY_INDEX, Params).

-spec(execute(Identifier :: atom() | binary(), BindType :: integer(), Params :: list()) ->
    {ok, Result :: list()} | badarg | {error, Reason :: binary()}).

execute(Identifier, BindType, Params) ->
    {ok, Tag} = async_execute(Identifier, BindType, Params),
    receive_response(Tag).

-spec(batch_async_execute(BatchType :: integer(), StmList :: list(), Options :: list()) ->
    {ok, Tag :: reference()} | badarg | {error, Reason :: binary()}).

batch_async_execute(BatchType, StmList, Options) ->
    gen_server:call(?MODULE, {batch_execute, BatchType, StmList, Options}).

-spec(batch_execute(BatchType :: integer(), StmList :: list(), Options :: list()) ->
    {ok, Result :: list()} | badarg | {error, Reason :: binary()}).

batch_execute(BatchType, StmList, Options) ->
    {ok, Tag} = batch_async_execute(BatchType, StmList, Options),
    receive_response(Tag).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:call(?MODULE, stop).

init([]) ->
    process_flag(trap_exit, true),

    ok = erlcass_prep_utils:create(),

    {ok, _LogPid} = erlcass_log:start_link(),
    ok = erlcass_nif:cass_cluster_create(),

    SessionRef = case application:get_env(erlcass, cluster_options) of
        {ok, ClusterOptions} ->
            erlcass_nif:cass_cluster_set_options(ClusterOptions),
            {ok, S} = erlcass_nif:cass_session_new(),

            case application:get_env(erlcass, keyspace) of
                {ok, Keyspace} ->
                    erlcass_nif:cass_session_connect_keyspace(S, self(), Keyspace);
                _ ->
                    erlcass_nif:cass_session_connect(S, self())
            end,

            receive
                {session_connected, _Pid} -> S
            after ?CONNECT_TIMEOUT ->
                erlcass_log:send(?CASS_LOG_ERROR, <<"Session connection timeout">>, []),
                error
            end;
        _ ->
            undefined
    end,

    if
        SessionRef == error ->
            {stop, session_connect_timeout, shutdown, #state{}};
        true ->
            {ok, #state{connected = false, session = SessionRef}}
    end.

handle_call({set_cluster_options, Options}, _From, State) ->
    Result = erlcass_nif:cass_cluster_set_options(Options),
    {reply, Result, State};

handle_call({create_session, Args}, From, State) ->
    {ok, SessionRef} = erlcass_nif:cass_session_new(),

    case lists:keyfind(keyspace, 1, Args) of
        {_Key, Value} ->
            ok = erlcass_nif:cass_session_connect_keyspace(SessionRef, From, Value);
        false ->
            ok = erlcass_nif:cass_session_connect(SessionRef, From)
    end,

    {noreply, State#state{session = SessionRef}};

handle_call(get_metrics, _From, State) ->
    {reply, erlcass_nif:cass_session_get_metrics(State#state.session), State};

handle_call({add_prepare_statement, Identifier, Query}, From, State) ->

    AlreadyExist = erlcass_prep_utils:does_exist(Identifier),

    if
        AlreadyExist ->
            {reply, already_exist, State};
        true ->
            ok = erlcass_nif:cass_session_prepare(State#state.session, Query, {From, Identifier}),
            {noreply, State}
    end;

handle_call({execute_normal_statements, StatementRef}, From, State) ->
    Tag = make_ref(),
    {FromPid, _} = From,
    Result = erlcass_nif:cass_session_execute(State#state.session, StatementRef, FromPid, Tag),
    {reply, {Result, Tag}, State};

handle_call({batch_execute, BatchType, StmList, Options}, From, State) ->
    {FromPid, _} = From,
    Result = erlcass_nif:cass_session_execute_batch(State#state.session, BatchType, filter_stm_list(StmList), Options, FromPid),
    {reply, Result, State};

handle_call(stop, _From, State) ->
    {stop, normal, shutdown, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({session_connected, {Status, FromPid}}, State) ->

    erlcass_log:send(?CASS_LOG_INFO, <<"Session connected result : ~p">>, [Status]),

    if
        Status =:= ok ->
            NewState = State#state{connected = true},
            gen_server:reply(FromPid, ok);
        true ->
            NewState = State,
            gen_server:reply(FromPid, Status)
    end,

    {noreply, NewState};

handle_info({prepared_statememt_result, Result, {From, Identifier}}, State) ->

    erlcass_log:send(?CASS_LOG_INFO, <<"Prepared statement id: ~p result: ~p">>, [Identifier, Result]),

    case Result of
        {ok, StatementRef} ->
            erlcass_prep_utils:set(Identifier, State#state.session, StatementRef),
            gen_server:reply(From, ok);
        _ ->
            gen_server:reply(From, Result)
    end,
    {noreply, State};

handle_info(Info, State) ->
    erlcass_log:send(?CASS_LOG_ERROR, <<"driver received: ~p">>, [Info]),
    {noreply, State}.

terminate(Reason, State) ->
    erlcass_log:send(?CASS_LOG_INFO, <<"Closing driver with reason: ~p">>, [Reason]),

    if
        State#state.connected =:= true ->

            {ok, Tag} = erlcass_nif:cass_session_close(State#state.session),

            receive
                {session_closed, Tag, Result} ->
                    erlcass_log:send(?CASS_LOG_INFO, <<"Session closed with result: ~p">>,[Result])

            after ?TIMEOUT ->
                erlcass_log:send(?CASS_LOG_ERROR, <<"Session closed timeout">>,[])
            end;

        true ->
            ok
    end,

    ok = erlcass_nif:cass_cluster_release().

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

receive_response(Tag) ->
    receive
        {execute_statement_result, Tag, Result} ->
            Result

        after ?TIMEOUT ->
            timeout
    end.

filter_stm_list(StmList) ->
    filter_stm_list(StmList, []).

filter_stm_list([], Acc) ->
    Acc;

filter_stm_list([H|T], Acc) when is_record(H, erlcass_stm) ->
    filter_stm_list(T, [H#erlcass_stm.stm | Acc]);

filter_stm_list([H|T], Acc) ->
    filter_stm_list(T, [H | Acc]).

