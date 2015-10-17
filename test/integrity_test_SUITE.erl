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
            prepared_bind_by_name_index,
            collection_types,
            nested_collections,
            tuples,
            batches,
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
            {protocol_version, 3},
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
            {heartbeat_interval, 30},
            {idle_timeout, 60},
            {default_consistency_level, ?CASS_CONSISTENCY_ONE}
        ]).

connect(_Config) ->
    ok = erlcass:create_session([]).

create_keyspace(_Config) ->
    erlcass:execute(<<"DROP KEYSPACE erlang_driver_test">>),
    {ok, []} = erlcass:execute(<<"CREATE KEYSPACE erlang_driver_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}">>).

create_table(_Config) ->
    {ok, []} = erlcass:execute(<<"CREATE TABLE erlang_driver_test.entries1 (id varchar, age int, email varchar, PRIMARY KEY(id))">>).

simple_insertion_roundtrip(_Config) ->

    Id = <<"hello">>,
    Age = 18,
    Email = <<"test@test.com">>,

    {ok, []} = erlcass:execute(<<"INSERT INTO erlang_driver_test.entries1(id, age, email) VALUES (?, ?, ?)">>, [
                                {?CASS_TEXT, Id},
                                {?CASS_INT, Age},
                                {?CASS_TEXT, Email}
    ]),

    {ok, [{Id, Age, Email}]} = erlcass:execute(<<"SELECT id, age, email FROM erlang_driver_test.entries1">>).

emptiness(_Config) ->

    {ok, []} = erlcass:execute(<<"update erlang_driver_test.entries1 set email = null where id = 'hello';">>),
    {ok, [{null}]} = erlcass:execute(<<"select email from erlang_driver_test.entries1 where id = 'hello';">>),

    {ok, []} = erlcass:execute(<<"update erlang_driver_test.entries1 set age = ? where id = 'hello';">>,[{?CASS_INT, null}]),
    {ok, [{null}]} = erlcass:execute(<<"select age from erlang_driver_test.entries1 where id = 'hello';">>).

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

    {ok, Tag2} = erlcass:async_execute(<<"SELECT id, age, email FROM erlang_driver_test.entries1">>),

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

all_datatypes(_Config) ->
    Cols = datatypes_columns([ascii, bigint, blob, boolean, decimal, double, float, int, timestamp, uuid, varchar, varint, timeuuid, inet]),
    CreationQ = <<"CREATE TABLE erlang_driver_test.entries2(",  Cols/binary, " PRIMARY KEY(col1));">>,
    {ok, []} = erlcass:execute(CreationQ),

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

    ok = erlcass:add_prepare_statement(insert_all_datatypes, InsertQuery),
    ok = erlcass:add_prepare_statement(select_all_datatypes, SelectQuery),

    AsciiString = "foo",
    BigIntNegative = -9223372036854775806,
    BooleanFalse = false,
    DecimalNegative = {erlang:integer_to_binary(1234), -5},
    DoubleNegative = -5.123513124122e-6,
    FloatNegative = -5.12351e-6,
    IntNegative = -2147483646,
    Varchar2 = <<"åäö"/utf8>>,
    Varint2 = erlang:integer_to_binary(123124211928301970128391280192830198049113123),

    {ok, []} = erlcass:execute(insert_all_datatypes, ?BIND_BY_NAME, [
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

    {ok, [Result2]} = erlcass:execute(select_all_datatypes, ?BIND_BY_NAME, [{<<"col1">>, AsciiString}]),

    BinAsciiString = list_to_binary(AsciiString),
    {BinAsciiString, BigIntNegative, Blob, BooleanFalse, DecimalNegative, DoubleNegative, _, IntNegative, Timestamp, Uuid, Varchar2, Varint2, Timeuuid, Inet} = Result2,
    ok.

prepared_bind_by_name_index(_Config) ->
    CreationQ = <<"CREATE TABLE erlang_driver_test.test_map(key int PRIMARY KEY, value map<text,text>)">>,
    {ok, []} = erlcass:execute(CreationQ),

    CollectionIndex1 = <<"my_index">>,
    CollectionValue1 = <<"my_value">>,
    CollectionIndex2 = <<"my_index_2">>,
    CollectionValue2 = <<"my_value_2">>,
    Key1 = 5,
    Key2 = 10,
    QueryInsert  = <<"UPDATE erlang_driver_test.test_map SET value[?] = ? WHERE key = ?">>,
    QuerySelect  = <<"SELECT value FROM erlang_driver_test.test_map where key = ?">>,

    ok = erlcass:add_prepare_statement(insert_test_bind, QueryInsert),
    ok = erlcass:add_prepare_statement(select_test_bind, QuerySelect),

    {ok, []} = erlcass:execute(insert_test_bind, ?BIND_BY_NAME, [
        {<<"key(value)">>, CollectionIndex1},
        {<<"value(value)">>, CollectionValue1},
        {<<"key">>, Key1}
    ]),

    {ok, []} = erlcass:execute(insert_test_bind, [CollectionIndex2, CollectionValue2, Key2]),
    {ok, [{[{CollectionIndex1, CollectionValue1}]}]} = erlcass:execute(select_test_bind, ?BIND_BY_NAME, [{<<"key">>, Key1}]),
    {ok, [{[{CollectionIndex2, CollectionValue2}]}]} = erlcass:execute(select_test_bind, [Key2]),
    ok.

collection_types(_Config) ->
    CreationQ = <<"CREATE TABLE erlang_driver_test.entries3(key varchar, numbers list<int>, names set<varchar>, phones map<int, varchar>, PRIMARY KEY(key));">>,
    ct:log("Executing : ~s~n", [CreationQ]),
    {ok, []} = erlcass:execute(CreationQ),

    Key1 = <<"somekeyhere_1">>,
    Key2 = <<"somekeyhere_2">>,
    Key3 = <<"somekeyhere_3">>,
    List = [1,2,3,4,5,6,7,8,9,0],
    Set = [<<"item1">>, <<"item2">>, <<"item3">>],
    Map = [{100, <<"418-123-4545">>}, {200, <<"555-555-5555">>}],
    InsertQ = <<"INSERT INTO erlang_driver_test.entries3(key, numbers, names, phones) values (?, ?, ?, ?);">>,
    SelectQ = <<"SELECT key, numbers, names, phones FROM erlang_driver_test.entries3 WHERE key = ?;">>,

    ok = erlcass:add_prepare_statement(insert_collection_types, InsertQ),
    ok = erlcass:add_prepare_statement(select_collection_types, SelectQ),

    %%insert using normal query, prepapred query (bind by name and bind by index)

    {ok, []} = erlcass:execute(InsertQ,
        [
            {?CASS_TEXT, Key1},
            {?CASS_LIST(?CASS_INT), List},
            {?CASS_SET(?CASS_TEXT), Set},
            {?CASS_MAP(?CASS_INT, ?CASS_TEXT), Map}
        ]),

    {ok, []} = erlcass:execute(insert_collection_types, ?BIND_BY_NAME,
        [
            {<<"key">>, Key2},
            {<<"numbers">>, List},
            {<<"names">>, Set},
            {<<"phones">>, Map}
        ]),

    {ok, []} = erlcass:execute(insert_collection_types, [Key3, List, Set, Map]),

    {ok, [Rs]} = erlcass:execute(SelectQ, [{?CASS_TEXT, Key1}]),
    {Key1, List, Set, Map} = Rs,
    {ok, [{Key2, List, Set, Map}]} = erlcass:execute(select_collection_types, ?BIND_BY_NAME, [{<<"key">>, Key2}]),
    {ok, [{Key3, List, Set, Map}]} = erlcass:execute(select_collection_types, ?BIND_BY_INDEX, [Key3]),

    ok.

nested_collections(_Config) ->
    CreationQ = <<"CREATE TABLE erlang_driver_test.nested_collections(key varchar, numbers list<frozen<map<int, varchar>>>, PRIMARY KEY(key))">>,
    ct:log("Executing : ~s~n", [CreationQ]),
    {ok, []} = erlcass:execute(CreationQ),

    Key1 = <<"somekeyhere_1">>,
    Key2 = <<"somekeyhere_2">>,
    Key3 = <<"somekeyhere_3">>,
    List = [[{100, <<"418-123-4545">>}, {200, <<"555-555-5555">>}], [{101, <<"1418-123">>}, {201, <<"5555-111">>}]],
    InsertQ = <<"INSERT INTO erlang_driver_test.nested_collections(key, numbers) values (?, ?)">>,
    SelectQ = <<"SELECT key, numbers FROM erlang_driver_test.nested_collections WHERE key = ?">>,

    ok = erlcass:add_prepare_statement(nest_insert_collection_types, InsertQ),
    ok = erlcass:add_prepare_statement(nest_select_collection_types, SelectQ),

    %%insert using normal query, prepapred query (bind by name and bind by index)

    {ok, []} = erlcass:execute(InsertQ,
        [
            {?CASS_TEXT, Key1},
            {?CASS_LIST(?CASS_MAP(?CASS_INT, ?CASS_TEXT)), List}]),

    {ok, []} = erlcass:execute(nest_insert_collection_types, ?BIND_BY_NAME,
        [
            {<<"key">>, Key2},
            {<<"numbers">>, List}
        ]),

    {ok, []} = erlcass:execute(nest_insert_collection_types, [Key3, List]),

    {ok, [{Key1, List}]} = erlcass:execute(SelectQ, [{?CASS_TEXT, Key1}]),
    {ok, [{Key2, List}]} = erlcass:execute(nest_select_collection_types, ?BIND_BY_NAME, [{<<"key">>, Key2}]),
    {ok, [{Key3, List}]} = erlcass:execute(nest_select_collection_types, ?BIND_BY_INDEX, [Key3]),
    ok.

tuples(_Config) ->
    CreationQ = <<"CREATE TABLE erlang_driver_test.tuples (key varchar, item1 frozen<tuple<text, list<int>>>, item2 frozen< tuple< tuple<text, bigint> > >, PRIMARY KEY(key));">>,
    ct:log("Executing : ~s~n", [CreationQ]),
    {ok, []} = erlcass:execute(CreationQ),

    Key1 = <<"somekeyhere_1">>,
    Key2 = <<"somekeyhere_2">>,
    Key3 = <<"somekeyhere_3">>,

    Item1 = {<<"sss">>, [1,2,3]},
    Item2 = {{<<"a">>, 1}},

    InsertQ = <<"INSERT INTO erlang_driver_test.tuples(key, item1, item2) values (?, ?, ?)">>,
    SelectQ = <<"SELECT key, item1, item2 FROM erlang_driver_test.tuples WHERE key = ?">>,

    ok = erlcass:add_prepare_statement(insert_tuple_types, InsertQ),
    ok = erlcass:add_prepare_statement(select_tuple_types, SelectQ),

    {ok, []} = erlcass:execute(InsertQ,
        [
            {?CASS_TEXT, Key1},
            {?CASS_TUPLE([?CASS_TEXT, ?CASS_LIST(?CASS_INT)]), Item1},
            {?CASS_TUPLE([?CASS_TUPLE([?CASS_TEXT, ?CASS_BIGINT])]), Item2}]),

    {ok, []} = erlcass:execute(insert_tuple_types, ?BIND_BY_NAME,
        [
            {<<"key">>, Key2},
            {<<"item1">>, Item1},
            {<<"item2">>, Item2}
        ]),

    {ok, []} = erlcass:execute(insert_tuple_types, [Key3, Item1, Item2]),

    {ok, [{Key1, Item1, Item2}]} = erlcass:execute(SelectQ, [{?CASS_TEXT, Key1}]),
    {ok, [{Key2, Item1, Item2}]} = erlcass:execute(select_tuple_types, ?BIND_BY_NAME, [{<<"key">>, Key2}]),
    {ok, [{Key3, Item1, Item2}]} = erlcass:execute(select_tuple_types, ?BIND_BY_INDEX, [Key3]),
    ok.

batches(_Config) ->
    {ok, []} = erlcass:execute(<<"TRUNCATE erlang_driver_test.entries1;">>),

    InsertStatement = <<"INSERT INTO erlang_driver_test.entries1(id, age, email) VALUES (?, ?, ?)">>,
    Id1 = <<"id_1">>,
    Id2 = <<"id_2">>,
    Age1 = 11,
    Age2 = 12,
    Email1 = <<"test1@test.com">>,
    Email2 = <<"test2@test.com">>,

    {ok, Stm1} = erlcass:create_statement(InsertStatement, [{?CASS_TEXT, Id1}, {?CASS_INT, Age1}, {?CASS_TEXT, Email1}]),
    ok = erlcass:add_prepare_statement(insert_prep, InsertStatement),

    {ok, Stm2} = erlcass:bind_prepared_statement(insert_prep),
    ok = erlcass:bind_prepared_params_by_name(Stm2, [{<<"id">>, Id2}, {<<"age">>, Age2}, {<<"email">>, Email2}]),

    {ok, []} = erlcass:batch_execute(?CASS_BATCH_TYPE_LOGGED, [Stm1, Stm2], [{consistency_level, ?CASS_CONSISTENCY_QUORUM}]),

    {ok, Result} = erlcass:execute(<<"SELECT id, age, email FROM erlang_driver_test.entries1">>),
    ListLength = 2,
    ListLength = length(Result),
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
    {ok, []} = erlcass:execute(<<"DROP KEYSPACE erlang_driver_test">>).


