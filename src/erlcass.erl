-module(erlcass).
-author("silviu.caragea").

-define(NOT_LOADED, not_loaded(?LINE)).
-define(TIMEOUT, 20000).
-define(CONNECT_TIMEOUT, 5000).

-behaviour(gen_server).

-on_load(load_nif/0).

-export([start_link/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([
    set_cluster_options/1,
    create_session/1,
    get_metrics/0,
    add_prepare_statement/2,

    async_execute/2,
    execute/2,
    batch_execute/3,
    batch_async_execute/3,

    create_statement/2,
    bind_prepared_statement/1,
    bind_prepared_params/2,
    async_execute_statement/1,
    execute_statement/1,

    uuid_gen_time/0,
    uuid_gen_random/0,
    uuid_gen_from_ts/1,
    uuid_min_from_ts/1,
    uuid_max_from_ts/1,
    uuid_get_ts/1,
    uuid_get_version/1
]).

-define(SERVER, ?MODULE).

-record(state, {session, connected, ets_prep, uuid_generator}).

-spec(set_cluster_options(OptionList :: list()) ->
    ok | badarg | {error, Reason :: binary()}).

set_cluster_options(Options) ->
    gen_server:call(?MODULE, {set_cluster_options, Options}).

-spec(create_session(Args :: list()) ->
    ok | badarg | {error, Reason :: binary()}).

create_session(Args) ->
    gen_server:call(?MODULE, {create_session, Args}, ?CONNECT_TIMEOUT).

-spec(get_metrics() ->
    {ok, MetricsList :: list()} | badarg | {error, Reason :: binary()}).

get_metrics() ->
    gen_server:call(?MODULE, get_metrics).

-spec(create_statement(Query :: binary() | {binary(), integer()}, BindParams :: list()) ->
    {ok, StatementRef :: reference()} | badarg | {error, Reason :: binary()}).

create_statement(Query, BindParams) ->
    nif_cass_statement_new(Query, BindParams).

-spec(add_prepare_statement(Identifier :: atom(), Query :: binary() | {binary(), integer()}) ->
    ok | already_exist | badarg | {error, Reason :: binary()}).

add_prepare_statement(Identifier, Query) ->
    gen_server:call(?MODULE, {prepare_statement, Identifier, Query}, ?TIMEOUT).

-spec(bind_prepared_statement(Identifier :: atom()) ->
    {ok, StatementRef :: reference()} | badarg | {error, Reason :: binary()}).

bind_prepared_statement(Identifier) ->
    gen_server:call(?MODULE, {bind_prepare_statement, Identifier}).

-spec(bind_prepared_params(StatementRef :: reference(), Params :: list()) ->
    ok | badarg | {error, Reason :: binary()}).

bind_prepared_params(StatementRef, Params) ->
    nif_cass_statement_bind_parameters(StatementRef, Params).

-spec(async_execute_statement(StatementRef :: reference()) ->
    {ok, Tag :: reference()} | badarg | {error, Reason :: binary()}).

async_execute_statement(StatementRef) ->
    gen_server:call(?MODULE, {execute_statement, StatementRef}).

-spec(execute_statement(StatementRef :: reference()) ->
    {ok, Result :: list()} | badarg | {error, Reason :: binary()}).

execute_statement(StatementRef) ->
    {ok, Tag} = async_execute_statement(StatementRef),
    receive_response(Tag).

-spec(async_execute(Identifier :: atom() | binary(), Params :: list()) ->
    {ok, Tag :: reference()} | badarg | {error, Reason :: binary()}).

async_execute(Identifier, Params) ->
    if
        is_atom(Identifier) ->
            {ok, Statement} = bind_prepared_statement(Identifier),
            ok = bind_prepared_params(Statement, Params);

        true ->
            {ok, Statement} = create_statement(Identifier, Params)
    end,

    async_execute_statement(Statement).

-spec(execute(Identifier :: atom() | binary(), Params :: list()) ->
    {ok, Result :: list()} | badarg | {error, Reason :: binary()}).

execute(Identifier, Params) ->
    {ok, Tag} = async_execute(Identifier, Params),
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

-spec(uuid_gen_time() ->
    {ok, Uuid :: binary()} | {error, Reason :: binary()}).

uuid_gen_time() ->
    gen_server:call(?MODULE, gen_time).

-spec(uuid_gen_random() ->
    {ok, Uuid :: binary()} | {error, Reason :: binary()}).

uuid_gen_random() ->
    gen_server:call(?MODULE, gen_random).

-spec(uuid_gen_from_ts(Ts :: integer()) ->
    {ok, Uuid :: binary()} | badarg | {error, Reason :: binary()}).

uuid_gen_from_ts(Ts) ->
    gen_server:call(?MODULE, {gen_from_time, Ts}).

-spec(uuid_min_from_ts(Ts :: integer()) ->
    {ok, Uuid :: binary()} | badarg | {error, Reason :: binary()}).

uuid_min_from_ts(Ts) ->
    nif_cass_uuid_min_from_time(Ts).

-spec(uuid_max_from_ts(Ts :: integer()) ->
    {ok, Uuid :: binary()} | badarg | {error, Reason :: binary()}).

uuid_max_from_ts(Ts) ->
    nif_cass_uuid_max_from_time(Ts).

-spec(uuid_get_ts(Uuid :: binary()) ->
    {ok, Ts :: integer()} | badarg | {error, Reason :: binary()}).

uuid_get_ts(Uuid) ->
    nif_cass_uuid_timestamp(Uuid).

-spec(uuid_get_version(Uuid :: binary()) ->
    {ok, Version :: integer()} | badarg | {error, Reason :: binary()}).

uuid_get_version(Uuid) ->
    nif_cass_uuid_version(Uuid).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:call(?MODULE, stop).

init([]) ->
    process_flag(trap_exit, true),
    {ok, UuidGenerator} = nif_cass_uuid_gen_new(),

    Tid = ets:new(erlcass_prepared_statements, [set, private, {read_concurrency, true}]),

    SessionRef = case application:get_env(erlcass, cluster_options) of
        {ok, ClusterOptions} ->
            nif_cass_cluster_set_options(ClusterOptions),
            {ok, S} = nif_cass_session_new(),
            case application:get_env(erlcass, keyspace) of
                {ok, Keyspace} -> nif_cass_session_connect_keyspace(S, self(), Keyspace);
                _ -> nif_cass_session_connect(S, self())
            end,

            receive
                {session_connected, _Pid} -> S
            after ?CONNECT_TIMEOUT ->
                io:format("Session connection timeout~n"),
                error
            end;
        _ ->
            undefined
    end,
    if SessionRef == error ->
        {stop, session_connect_timeout, shutdown, #state{}};
    true ->
        {ok, #state{connected = false, ets_prep = Tid, uuid_generator = UuidGenerator, session = SessionRef}}
    end.

handle_call(gen_time, _From, State) ->
    {reply, nif_cass_uuid_gen_time(State#state.uuid_generator), State};

handle_call(gen_random, _From, State) ->
    {reply, nif_cass_uuid_gen_random(State#state.uuid_generator), State};

handle_call({gen_from_time, Ts}, _From, State) ->
    {reply, nif_cass_uuid_gen_from_time(State#state.uuid_generator, Ts), State};

handle_call({set_cluster_options, Options}, _From, State) ->
    Result = nif_cass_cluster_set_options(Options),
    {reply, Result, State};

handle_call({create_session, Args}, From, State) ->
    {ok, SessionRef} = nif_cass_session_new(),

    case lists:keyfind(keyspace, 1, Args) of
        {_Key, Value} ->
            ok = nif_cass_session_connect_keyspace(SessionRef, From, Value);
        false ->
            ok = nif_cass_session_connect(SessionRef, From)
    end,

    {noreply, State#state{session = SessionRef}};

handle_call(get_metrics, _From, State) ->
    {reply, nif_cass_session_get_metrics(State#state.session), State};

handle_call({prepare_statement, Identifier, Query}, From, State) ->

    AlreadyExist = prepare_statement_exist(State#state.ets_prep, Identifier),

    if
        AlreadyExist ->
            {reply, already_exist, State};
        true ->
            ok = nif_cass_session_prepare(State#state.session, Query, {From, Identifier}),
            {noreply, State}
    end;

handle_call({bind_prepare_statement, Identifier}, _From, State) ->
    PrepStatement = prepare_statement_get(State#state.ets_prep, Identifier),

    if
        PrepStatement =/= undefined ->
            {ok, Statement} = nif_cass_prepared_bind(PrepStatement),
            Result = {ok, Statement};
        true ->
            Result = {error, undefined}
    end,

    {reply, Result, State};

handle_call(stop, _From, State) ->
    {stop, normal, shutdown, State};

handle_call({execute_statement, StatementRef}, From, State) ->
    Tag = make_ref(),
    {FromPid, _} = From,
    Result = nif_cass_session_execute(State#state.session, StatementRef, FromPid, Tag),
    {reply, {Result, Tag}, State};

handle_call({batch_execute, BatchType, StmList, Options}, From, State) ->
    {FromPid, _} = From,
    Result = nif_cass_session_execute_batch(State#state.session, BatchType, StmList, Options, FromPid),
    {reply, Result, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({session_connected, {Status, FromPid}}, State) ->

    io:format("Session connected result : ~p ~n", [Status]),

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

    io:format("Prepared statement id: ~p result: ~p ~n", [Identifier, Result]),

    case Result of
        {ok, StatementRef} ->
            prepare_statement_set(State#state.ets_prep, Identifier, StatementRef),
            gen_server:reply(From, ok);
        _ ->
            gen_server:reply(From, Result)
    end,
    {noreply, State};

handle_info(Info, State) ->
    io:format("driver received: ~p", [Info]),
    {noreply, State}.

terminate(Reason, State) ->
    io:format("Closing driver with reason: ~p ~n", [Reason]),

    if
        State#state.connected =:= true ->

            {ok, Tag} = nif_cass_session_close(State#state.session),

            receive
                {session_closed, Tag, Result} ->
                    io:format("Session closed with result: ~p ~n",[Result])

            after ?TIMEOUT ->
                io:format("Session closed timeout",[])
            end;

        true ->
            ok
    end,

    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

receive_response(Tag) ->
    receive
        {execute_statement_result, Tag, Result} ->
            Result

        after ?TIMEOUT ->
            timeout
    end.

%% helper functions to store prepare statements

prepare_statement_set(EtsTid, Identifier, StatementRef) ->
    true = ets:insert(EtsTid, {Identifier, StatementRef}).

prepare_statement_get(EtsTid, Identifier) ->
    case ets:lookup(EtsTid, Identifier) of
        [{Identifier, Value}] ->
            Value;
        [] ->
            undefined
    end.

prepare_statement_exist(EtsTid, Identifier) ->
    case ets:lookup(EtsTid, Identifier) of
        [{Identifier, _}] ->
            true;
        [] ->
            false
    end.

%% nif functions

load_nif() ->
    SoName = get_nif_library_path(),
    io:format("Loading library: ~p ~n", [SoName]),
    ok = erlang:load_nif(SoName, 0).

get_nif_library_path() ->
    case code:priv_dir(?MODULE) of
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

nif_cass_cluster_set_options(_OptionList) ->
    ?NOT_LOADED.

nif_cass_session_new() ->
    ?NOT_LOADED.

nif_cass_session_connect(_SessionRef, _FromPid) ->
    ?NOT_LOADED.

nif_cass_session_connect_keyspace(_SessionRef, _FromPid, _Keyspace) ->
    ?NOT_LOADED.

nif_cass_session_close(_SessionRef) ->
    ?NOT_LOADED.

nif_cass_session_prepare(_SessionRef, _Query, _Info) ->
    ?NOT_LOADED.

nif_cass_prepared_bind(_PrepStatementRef) ->
    ?NOT_LOADED.

nif_cass_statement_new(_Query, _Params) ->
    ?NOT_LOADED.

nif_cass_statement_bind_parameters(_StatementRef, _Args)->
    ?NOT_LOADED.

nif_cass_session_execute(_SessionRef, _StatementRef, _FromPid, _Tag) ->
    ?NOT_LOADED.

nif_cass_session_execute_batch(_SessionRef, _BatchType, _StmList, _Options, _Pid) ->
    ?NOT_LOADED.

nif_cass_session_get_metrics(_SessionRef) ->
    ?NOT_LOADED.

nif_cass_uuid_gen_new() ->
    ?NOT_LOADED.

nif_cass_uuid_gen_time(_Generator) ->
    ?NOT_LOADED.

nif_cass_uuid_gen_random(_Generator) ->
    ?NOT_LOADED.

nif_cass_uuid_gen_from_time(_Generator, _Ts) ->
    ?NOT_LOADED.

nif_cass_uuid_min_from_time(_Ts) ->
    ?NOT_LOADED.

nif_cass_uuid_max_from_time(_Ts) ->
    ?NOT_LOADED.

nif_cass_uuid_timestamp(_Uuid) ->
    ?NOT_LOADED.

nif_cass_uuid_version(_Uuid) ->
    ?NOT_LOADED.