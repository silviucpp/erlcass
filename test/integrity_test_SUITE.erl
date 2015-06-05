-module(integrity_test_SUITE).
-author("silviu.caragea").

-include_lib("common_test/include/ct.hrl").
-include("erlcass.hrl").

-compile(export_all).

-define(CONTACT_POINTS, <<"127.0.0.1">>).

%%ct_run -suite integrity_test_SUITE -pa ebin -include include

all() ->
[
    {group, database}
].

groups() ->
[
    {
        database, [sequence],
        [
            set_cluster_options,
            connect,
            create_keyspace,
            create_table,
            simple_insertion_roundtrip,
            async_insertion_roundtrip,
            emptiness,
            all_datatypes,
            collection_types,
            uuid_testing,
            get_metrics,
            drop_keyspace
        ]
    }
].

init_per_suite(Config) ->
    ok = application:start(erlcass),
    Config.

end_per_suite(_Config) ->
    application:stop(erlcass).

set_cluster_options(_Config) ->
    ok = erlcass:set_cluster_options(
        [
            {contact_points, ?CONTACT_POINTS},
            {port, 9042},
            {protocol_version, 2},
            {number_threads_io, 1},
            {queue_size_io, 4096},
            {queue_size_event, 4096},
            {core_connections_host, 1},
            {max_connections_host, 2},
            {reconnect_wait_time, 2000},
            {max_concurrent_creation, 1},
            {max_requests_threshold, 100},
            {requests_per_flush, 128},
            {write_bytes_high_watermark, 65536},
            {write_bytes_low_watermark, 32768},
            {pending_requests_high_watermark, 128},
            {pending_requests_low_watermark, 64},
            {connect_timeout, 5000},
            {request_timeout, 12000},
            %{credentials, {<<"username">>, <<"password">>}},
            {load_balance_round_robin, true},
            %{load_balance_dc_aware, {<<"dc-beta">>, 0, false}},
            {token_aware_routing, true},
            {latency_aware_routing, {true, {2.0, 100, 10000, 100 , 50}}},
            {tcp_nodelay, true},
            {tcp_keepalive, {true, 60}},
            {default_consistency_level, ?CASS_CONSISTENCY_ONE}
        ]).

connect(_Config) ->
    ok = erlcass:create_session([]).

create_keyspace(_Config) ->
    {ok, []} = erlcass:execute(<<"CREATE KEYSPACE erlang_driver_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}">>,[]).

create_table(_Config) ->
    {ok, []} = erlcass:execute(<<"CREATE TABLE erlang_driver_test.entries1 (id varchar, age int, email varchar, PRIMARY KEY(id))">>, []).

simple_insertion_roundtrip(_Config) ->

    Id = <<"hello">>,
    Age = 18,
    Email = <<"test@test.com">>,

    {ok, []} = erlcass:execute(<<"INSERT INTO erlang_driver_test.entries1(id, age, email) VALUES (?, ?, ?)">>, [
                                {?CASS_TEXT, Id},
                                {?CASS_INT, Age},
                                {?CASS_TEXT, Email}
    ]),

    {ok, [{Id, Age, Email}]} = erlcass:execute(<<"SELECT id, age, email FROM erlang_driver_test.entries1">>,[]).

emptiness(_Config) ->

    {ok, []} = erlcass:execute(<<"update erlang_driver_test.entries1 set email = null where id = 'hello';">>,[]),
    {ok, [{null}]} = erlcass:execute(<<"select email from erlang_driver_test.entries1 where id = 'hello';">>, []),

    {ok, []} = erlcass:execute(<<"update erlang_driver_test.entries1 set age = ? where id = 'hello';">>,[{?CASS_INT, null}]),
    {ok, [{null}]} = erlcass:execute(<<"select age from erlang_driver_test.entries1 where id = 'hello';">>, []).

async_insertion_roundtrip(_Config) ->

    Id = <<"hello_async">>,
    Age = 32,
    Email = <<"zz@test.com">>,

    {ok, Tag} = erlcass:async_execute(<<"INSERT INTO erlang_driver_test.entries1(id, age, email) VALUES (?, ?, ?)">>, [
        {?CASS_TEXT, Id},
        {?CASS_INT, Age},
        {?CASS_TEXT, Email}
    ]),

    receive
        {execute_statement_result, Tag, Result} ->
            Result = {ok, []}

        after 10000 ->
            ct:fail("Timeout on executing query ~n", [])
    end,

    {ok, Tag2} = erlcass:async_execute(<<"SELECT id, age, email FROM erlang_driver_test.entries1">>,[]),

    receive
        {execute_statement_result, Tag2, Result2} ->
            {ok, List} = Result2,
            {Id, Age, Email} = lists:last(List)
        after 10000 ->
            ct:fail("Timeout on executing query ~n", [])
    end,

    ok.

datatypes_columns(Cols) ->
    datatypes_columns(1, Cols, <<>>).

datatypes_columns(_I, [], Bin) -> Bin;
datatypes_columns(I, [ColumnType|Rest], Bin) ->
    Column = list_to_binary(io_lib:format("col~B ~s, ", [I, ColumnType])),
    datatypes_columns(I+1, Rest, << Bin/binary, Column/binary >>).

%limitation: float numbers from erlang are coming back with double precision.
%seems the c++ driver has an issue with binding varint by name in a prepared statement https://datastax-oss.atlassian.net/browse/CPP-272

all_datatypes(_Config) ->
    Cols = datatypes_columns([ascii, bigint, blob, boolean, decimal, double, float, int, timestamp, uuid, varchar, varint, timeuuid, inet]),
    CreationQ = <<"CREATE TABLE erlang_driver_test.entries2(",  Cols/binary, " PRIMARY KEY(col1));">>,
    {ok, []} = erlcass:execute(CreationQ, []),

    AsciiValBin = <<"hello">>,
    BigIntPositive = 9223372036854775807,
    Blob = <<1,2,3,4,5,6,7,8,9,10>>,
    BooleanTrue = true,
    DecimalPositive = {erlang:integer_to_binary(1234), 5},
    DoublePositive =  5.1235131241221e-6,
    FloatPositive = 5.12351e-6,
    IntPositive = 2147483647,
    Timestamp = 2147483647,
    {ok, Uuid} = erlcass:uuid_gen_random(),
    Varchar1 = <<"Юникод"/utf8>>,
    Varint1 = erlang:integer_to_binary(1928301970128391280192830198049113123),
    {ok, Timeuuid} = erlcass:uuid_gen_time(),
    Inet = <<"127.0.0.1">>,

    InsertQuery = <<"INSERT INTO erlang_driver_test.entries2(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)">>,
    SelectQuery = <<"SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14 FROM erlang_driver_test.entries2 WHERE col1 =?">>,

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
            {?CASS_INET, Inet}
        ]),

    {ok, [Result]} = erlcass:execute(SelectQuery, [{?CASS_TEXT, AsciiValBin}]),

    {AsciiValBin, BigIntPositive, Blob, BooleanTrue, DecimalPositive, DoublePositive, _, IntPositive, Timestamp, Uuid, Varchar1, Varint1, Timeuuid, Inet} = Result,

    ok = erlcass:add_prepare_statement(insert_all_datatypes, InsertQuery, [
        {<<"col1">>, ?CASS_TEXT},
        {<<"col2">>, ?CASS_BIGINT},
        {<<"col3">>, ?CASS_BLOB},
        {<<"col4">>, ?CASS_BOOLEAN},
        {<<"col5">>, ?CASS_DECIMAL},
        {<<"col6">>, ?CASS_DOUBLE},
        {<<"col7">>, ?CASS_FLOAT},
        {<<"col8">>, ?CASS_INT},
        {<<"col9">>, ?CASS_BIGINT},
        {<<"col10">>, ?CASS_UUID},
        {<<"col11">>, ?CASS_TEXT},
        {<<"col12">>, ?CASS_BLOB},
        {<<"col13">>, ?CASS_UUID},
        {<<"col14">>, ?CASS_INET}
    ]),

    ok = erlcass:add_prepare_statement(select_all_datatypes, SelectQuery, [{<<"col1">>, ?CASS_TEXT}]),

    AsciiString = "foo",
    BigIntNegative = -9223372036854775806,
    BooleanFalse = false,
    DecimalNegative = {erlang:integer_to_binary(1234), -5},
    DoubleNegative = -5.123513124122e-6,
    FloatNegative = -5.12351e-6,
    IntNegative = -2147483646,
    Varchar2 = <<"åäö"/utf8>>,
    Varint2 = null, %erlang:integer_to_binary(123124211928301970128391280192830198049113123),

    {ok, []} = erlcass:execute(insert_all_datatypes, [
        {<<"col1">>, AsciiString},
        {<<"col2">>, BigIntNegative},
        {<<"col3">>, Blob},
        {<<"col4">>, BooleanFalse},
        {<<"col5">>, DecimalNegative},
        {<<"col6">>, DoubleNegative},
        {<<"col7">>, FloatNegative},
        {<<"col8">>, IntNegative},
        {<<"col9">>, Timestamp},
        {<<"col10">>, Uuid},
        {<<"col11">>, Varchar2},
        {<<"col12">>, Varint2},
        {<<"col13">>, Timeuuid},
        {<<"col14">>, Inet}
    ]),

    {ok, [Result2]} = erlcass:execute(select_all_datatypes, [{<<"col1">>, AsciiString}]),

    BinAsciiString = list_to_binary(AsciiString),
    {BinAsciiString, BigIntNegative, Blob, BooleanFalse, DecimalNegative, DoubleNegative, _, IntNegative, Timestamp, Uuid, Varchar2, Varint2, Timeuuid, Inet} = Result2,
    ok.

collection_types(_Config) ->
    CreationQ = <<"CREATE TABLE erlang_driver_test.entries3(key varchar, numbers list<int>, names set<varchar>, phones map<varchar, varchar>, PRIMARY KEY(key));">>,
    ct:log("Executing : ~s~n", [CreationQ]),
    {ok, []} = erlcass:execute(CreationQ, []),

    Key = <<"somekeyhere">>,
    List = [1,2,3,4,5,6,7,8,9,0],
    Set = [<<"item1">>, <<"item2">>, <<"item3">>],
    Map = [{<<"home">>, <<"418-123-4545">>}, {<<"work">>, <<"555-555-5555">>}],

    {ok, []} = erlcass:execute(<<"INSERT INTO erlang_driver_test.entries3(key, numbers, names, phones) values (?, ?, ?, ?);">>,
        [
            {?CASS_TEXT, Key},
            {?CASS_LIST(?CASS_INT), List},
            {?CASS_SET(?CASS_TEXT), Set},
            {?CASS_MAP(?CASS_TEXT, ?CASS_TEXT), Map}
        ]),

    {ok, [Result]} = erlcass:execute(<<"SELECT key, numbers, names, phones FROM erlang_driver_test.entries3;">>, []),
    {Key, List, Set, Map} = Result,
    ok.

uuid_testing(_Config) ->
    Ts = 2147483647,
    {ok, _} = erlcass:uuid_gen_time(),
    {ok, _} = erlcass:uuid_gen_random(),
    {ok, UuidTs} = erlcass:uuid_gen_from_ts(Ts),
    {ok, Ts} = erlcass:uuid_get_ts(UuidTs),
    {ok, 1} = erlcass:uuid_get_version(UuidTs),
    {ok, _} = erlcass:uuid_max_from_ts(Ts),
    {ok, _} = erlcass:uuid_min_from_ts(Ts),
    ok.

get_metrics(_Config) ->
    {ok, _} = erlcass:get_metrics(),
    ok.

drop_keyspace(_Config) ->
    {ok, []} = erlcass:execute(<<"DROP KEYSPACE erlang_driver_test">>,[]).


