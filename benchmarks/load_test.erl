-module(load_test).

-include("erlcass.hrl").

-export([
    profile/2,
    profile/3,
    prepare_load_test_table/0
]).

-define(KEYSPACE, <<"load_test_erlcass">>).
-define(QUERY, {<<"SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14 FROM test_table WHERE col1 =?">>, ?CASS_CONSISTENCY_ONE}).
-define(QUERY_ARGS, [<<"hello">>]).
-define(CLUSTER_NAME, <<"dc-beta">>).

start() ->
    case application:ensure_all_started(erlcass) of
        {ok, [_|_]} ->
            ok;
        UnexpectedError ->
            UnexpectedError
    end.

prepare_load_test_table() ->
    start(),

    erlcass:query(<<"DROP KEYSPACE ", (?KEYSPACE)/binary>>),
    ok = erlcass:query(<<"CREATE KEYSPACE ", (?KEYSPACE)/binary, " WITH replication = {'class': 'NetworkTopologyStrategy', '", (?CLUSTER_NAME)/binary, "': 3  }">>),

    Cols = datatypes_columns([ascii, bigint, blob, boolean, decimal, double, float, int, timestamp, uuid, varchar, varint, timeuuid, inet]),
    Sql = <<"CREATE TABLE ", (?KEYSPACE)/binary, ".test_table(",  Cols/binary, " PRIMARY KEY(col1));">>,
    io:format(<<"exec: ~p ~n">>,[Sql]),
    ok = erlcass:query(Sql),

    InsertQuery = <<"INSERT INTO ", (?KEYSPACE)/binary, ".test_table(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)">>,
    ok = erlcass:add_prepare_statement(add_load_test_record, InsertQuery),

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

    ok = erlcass:execute(add_load_test_record, [
            AsciiValBin,
            BigIntPositive,
            Blob,
            BooleanTrue,
            DecimalPositive,
            DoublePositive,
            FloatPositive,
            IntPositive,
            Timestamp,
            Uuid,
            Varchar1,
            Varint1,
            Timeuuid,
            Inet
    ]).

profile(NrProc, RequestsNr) ->
    start(),
    profile(NrProc, RequestsNr, 1).

profile(NrProc, RequestsNr, RepeatNumber) ->
    start(),
    Fun = fun() -> do_profiling(NrProc, RequestsNr, RepeatNumber) end,
    spawn(Fun).

do_profiling(NrProc, RequestsNr, RepeatNumber) ->
    erlcass:add_prepare_statement(execute_query, ?QUERY),

    eprof:start(),
    eprof:start_profiling([self()]),
    ok = application:set_env(erlcass, keyspace, ?KEYSPACE),
    start(),

    ProcsList = lists:seq(1, NrProc),
    {Time, _} = timer:tc( fun() -> run_test(RepeatNumber, NrProc, RequestsNr, ProcsList) end),

    io:format("Cpp Driver Metrics: ~p ~n", [erlcass:get_metrics()]),
    eprof:stop_profiling(),
    eprof:analyze(total),
    io:format("Time to complete: ~p ms ~n", [Time/1000]).

run_test(0, _NrProc, _RequestsNr, _ProcsList) ->
    ok;
run_test(RepeatNr, NrProc, RequestsNr, ProcsList) ->
    load_test(NrProc, RequestsNr, ProcsList),
    run_test(RepeatNr -1, NrProc, RequestsNr, ProcsList).

load_test(1, NrReq, _ProcsList) ->
    producer_loop(NrReq),
    consumer_loop(0, 0, 0, NrReq);
load_test(NrProcesses, NrReq, ProcsList) ->
    ReqPerProcess = round(NrReq/NrProcesses),
    Self = self(),

    Fun = fun() ->
        producer_loop(ReqPerProcess),
        consumer_loop(0, 0, 0, ReqPerProcess),
        Self ! {self(), done}
    end,

    wait_finish(Fun, ProcsList).

wait_finish(Fun, ProcsList) ->
    Pids = [spawn_link(Fun) || _ <- ProcsList],
    [receive {Pid, done} -> ok end || Pid <- Pids],
    ok.

consumer_loop(TotalResults, SuccessResults, FailedResults, 0) ->
    io:format("**** Test Completed => results: ~p success: ~p failed: ~p **** ~n",[TotalResults, SuccessResults, FailedResults]),
    ok;
consumer_loop(TotalResults, SuccessResults, FailedResults, LimitReq) ->
    receive

    %success results
        {execute_statement_result, _Tag, {ok, _Cols, _Rows}} ->
            %io:format("Result:~p ~n", [Result]),
            display_progress(TotalResults),
            consumer_loop(TotalResults +1 , SuccessResults +1, FailedResults, LimitReq -1);

    %failed results
        {execute_statement_result, _Tag, Result} ->
            io:format("Result:~p ~n", [Result]),
            display_progress(TotalResults),
            consumer_loop(TotalResults +1 , SuccessResults, FailedResults + 1, LimitReq -1)
    end.

producer_loop(0) ->
    ok;
producer_loop(NumRequests) ->
    erlcass:async_execute(execute_query, ?QUERY_ARGS),
    producer_loop(NumRequests -1).

display_progress(Results) ->
    if
        Results rem 10000 =:= 0 ->
            io:format("Executed ~p requests ~n", [Results]);
        true ->
            ok
    end.

datatypes_columns(Cols) ->
    datatypes_columns(1, Cols, <<>>).

datatypes_columns(_I, [], Bin) -> Bin;
datatypes_columns(I, [ColumnType|Rest], Bin) ->
    Column = list_to_binary(io_lib:format("col~B ~s, ", [I, ColumnType])),
    datatypes_columns(I+1, Rest, << Bin/binary, Column/binary >>).
