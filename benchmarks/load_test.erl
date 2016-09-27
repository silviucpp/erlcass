-module(load_test).
-author("silviu.caragea").

-include("erlcass.hrl").

-export([start/0, profile/3, profile/4, prepare_load_test_table/0]).

-define(KEYSPACE, <<"load_test_erlcass">>).
-define(CONTACT_POINTS, <<"172.17.3.129,172.17.3.130,172.17.3.131">>).
-define(CLUSTER_NAME, <<"dc-beta">>).
-define(QUERY, {<<"SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14 FROM test_table WHERE col1 =?">>, ?CASS_CONSISTENCY_ONE}).
-define(QUERY_ARGS, [<<"hello">>]).

start() ->
    Result = application:start(erlcass),

    if
        Result =:= ok ->

            ok = erlcass:set_cluster_options(
            [
                {contact_points, ?CONTACT_POINTS},
                {load_balance_dc_aware, {?CLUSTER_NAME, 0, false}},
                {latency_aware_routing, true},
                {token_aware_routing, true},
                {number_threads_io, 8},
                {queue_size_io, 128000},
                {core_connections_host, 5},
                {max_connections_host, 5},
                {tcp_nodelay, true},
                {tcp_keepalive, {true, 60}},
                {pending_requests_high_watermark, 128000}
            ]),
            true;
        true ->
            false
    end.

datatypes_columns(Cols) ->
    datatypes_columns(1, Cols, <<>>).

datatypes_columns(_I, [], Bin) -> Bin;
datatypes_columns(I, [ColumnType|Rest], Bin) ->
    Column = list_to_binary(io_lib:format("col~B ~s, ", [I, ColumnType])),
    datatypes_columns(I+1, Rest, << Bin/binary, Column/binary >>).

prepare_load_test_table() ->
    start(),
    ok = erlcass:create_session([]),
    erlcass:execute(<<"DROP KEYSPACE ", (?KEYSPACE)/binary>>),
    {ok, []} = erlcass:execute(<<"CREATE KEYSPACE ", (?KEYSPACE)/binary, " WITH replication = {'class': 'NetworkTopologyStrategy', '", (?CLUSTER_NAME)/binary, "': 3  }">>),

    Cols = datatypes_columns([ascii, bigint, blob, boolean, decimal, double, float, int, timestamp, uuid, varchar, varint, timeuuid, inet]),
    Sql = <<"CREATE TABLE ", (?KEYSPACE)/binary, ".test_table(",  Cols/binary, " PRIMARY KEY(col1));">>,
    io:format(<<"exec: ~p ~n">>,[Sql]),
    {ok, []} = erlcass:execute(Sql),

    InsertQuery = <<"INSERT INTO ", (?KEYSPACE)/binary, ".test_table(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)">>,

    AsciiValBin = <<"hello">>,
    BigIntPositive = 9223372036854775807,
    Blob = <<1,2,3,4,5,6,7,8,9,10>>,
    BooleanTrue = true,
    DecimalPositive = {erlang:integer_to_binary(1234), 5},
    DoublePositive =  5.1235131241221e-6,
    FloatPositive = 5.12351e-6,
    IntPositive = 2147483647,
    Timestamp = 2147483647,
    {ok, Uuid} = erlcass_uuid:gen_random(),
    Varchar1 = <<"Юникод"/utf8>>,
    Varint1 = erlang:integer_to_binary(1928301970128391280192830198049113123),
    {ok, Timeuuid} = erlcass_uuid:gen_time(),
    Inet = <<"127.0.0.1">>,

    {ok, []} = erlcass:execute(InsertQuery,
        [
            {?CASS_TEXT, AsciiValBin},
            {?CASS_BIGINT, BigIntPositive},
            {?CASS_BLOB, Blob},
            {?CASS_BOOLEAN, BooleanTrue},
            {?CASS_DECIMAL, DecimalPositive},
            {?CASS_DOUBLE, DoublePositive},
            {?CASS_FLOAT, FloatPositive},
            {?CASS_INT, IntPositive},
            {?CASS_BIGINT, Timestamp},
            {?CASS_UUID, Uuid},
            {?CASS_TEXT, Varchar1},
            {?CASS_BLOB, Varint1},
            {?CASS_UUID, Timeuuid},
            {?CASS_INET, Inet}]).

display_progress(Results) ->
    if
        Results rem 10000 =:= 0 ->
            io:format("Executed ~p requests ~n", [Results]);
        true ->
            ok
    end.

consumer_loop(TotalResults, SuccessResults, FailedResults, 0) ->
    io:format("**** Test Completed => results: ~p success: ~p failed: ~p **** ~n",[TotalResults, SuccessResults, FailedResults]),
    ok;

consumer_loop(TotalResults, SuccessResults, FailedResults, LimitReq) ->
    receive

    %success results
        {execute_statement_result, _Tag, {ok, _Result}} ->
            %io:format("Result:~p ~n", [Result]),
            display_progress(TotalResults),
            consumer_loop(TotalResults +1 , SuccessResults +1, FailedResults, LimitReq -1);

    %failed results
        {execute_statement_result, _Tag, Result} ->
            io:format("Result:~p ~n", [Result]),
            display_progress(TotalResults),
            consumer_loop(TotalResults +1 , SuccessResults, FailedResults + 1, LimitReq -1)
    end.

producer_loop(_St, 0, _UseOneSt) ->
    ok;

producer_loop(St, NumRequests, UseOneSatement) ->

    case UseOneSatement of
        true ->
            erlcass:async_execute_statement(St);
        false ->
            erlcass:async_execute(execute_query, ?QUERY_ARGS)
    end,

    producer_loop(St, NumRequests -1, UseOneSatement).

get_statement(UseOneSatement) ->
    case UseOneSatement of
        true ->
            {ok, Statement} = erlcass:bind_prepared_statement(execute_query),
            ok = erlcass:bind_prepared_params_by_index(Statement, ?QUERY_ARGS),
            Statement;
        false ->
            undefined
    end.

load_test(St, 1, NrReq, UseOneSatement) ->
    producer_loop(St, NrReq, UseOneSatement),
    consumer_loop(0, 0, 0, NrReq);

load_test(St, NrProcesses, NrReq, UseOneSatement) ->
    ReqPerProcess = round(NrReq/NrProcesses),

    Fun = fun() ->
        producer_loop(St, ReqPerProcess, UseOneSatement),
        consumer_loop(0, 0, 0, ReqPerProcess)
    end,

    multi_spawn:do_work(Fun, NrProcesses).

run_test(0, _St, _NrProc, _RequestsNr, _UseOneSatement) ->
    ok;

run_test(RepeatNr, St, NrProc, RequestsNr, UseOneSatement) ->
    load_test(St, NrProc, RequestsNr, UseOneSatement),
    run_test(RepeatNr -1, St,  NrProc, RequestsNr, UseOneSatement).

profile(NrProc, RequestsNr, UseOneSatement) ->
    profile(NrProc, RequestsNr, 1, UseOneSatement).

profile(NrProc, RequestsNr, RepeatNumber, UseOneSatement) ->
    Fun = fun() -> do_profiling(NrProc, RequestsNr, RepeatNumber, UseOneSatement) end,
    spawn(Fun).

do_profiling(NrProc, RequestsNr, RepeatNumber, UseOneSatement) ->
    eprof:start(),
    eprof:start_profiling([self()]),
    Rs = start(),

    case Rs of
        true ->
            ok = erlcass:create_session([{keyspace, ?KEYSPACE}]),
            ok = erlcass:add_prepare_statement(execute_query, ?QUERY);
        false ->
            ok
    end,

    St = get_statement(UseOneSatement),

    {Time, _} = timer:tc( fun() -> run_test(RepeatNumber, St, NrProc, RequestsNr, UseOneSatement) end),

    io:format("Cpp Driver Metrics: ~p ~n", [erlcass:get_metrics()]),
    eprof:stop_profiling(),
    eprof:analyze(total),
    io:format("Time to complete: ~p ms ~n", [Time/1000]).