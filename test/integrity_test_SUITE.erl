-module(integrity_test_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("erlcass.hrl").

-compile(export_all).

all() -> [
    {group, database}
].

groups() -> [
    {database, [sequence], [
        create_keyspace,
        create_table,
        simple_insertion_roundtrip,
        async_insertion_roundtrip,
        emptiness,
        all_datatypes,
        prepared_bind_by_name_index,
        async_custom_tag,
        collection_types,
        fire_and_forget,
        nested_collections,
        tuples,
        batches,
        test_udt,
        erlcass_crash,
        uuid_testing,
        date_time_testing,
        get_metrics,
        drop_keyspace
    ]}
].

suite() ->
    [{timetrap, {seconds, 40}}].

init_per_suite(Config) ->
    {ok,  _} = application:ensure_all_started(erlcass),
    Config.

end_per_suite(_Config) ->
    application:stop(erlcass).

create_keyspace(_Config) ->
    case erlcass:query(<<"DROP KEYSPACE erlang_driver_test">>) of
        ok ->
            ok;
        {error,<<"Cannot drop non existing", _/binary>>} ->
            ok
    end,
    ok = erlcass:query(<<"CREATE KEYSPACE erlang_driver_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}">>).

create_table(_Config) ->
    ok = erlcass:query(<<"CREATE TABLE erlang_driver_test.entries1 (id varchar, age int, email varchar, PRIMARY KEY(id))">>).

simple_insertion_roundtrip(_Config) ->
    Id = <<"hello">>,
    Age = 18,
    Email = <<"test@test.com">>,

    ok = erlcass:query(<<"INSERT INTO erlang_driver_test.entries1(id, age, email) VALUES ('",
        Id/binary,"',",
        (integer_to_binary(18))/binary, ", '",
        Email/binary, "')">>),

    Cols = [{<<"id">>, text}, {<<"age">>, int}, {<<"email">>, text}],
    {ok, Cols, [[Id, Age, Email]]} = erlcass:query(<<"SELECT id, age, email FROM erlang_driver_test.entries1">>).

emptiness(_Config) ->
    ok = erlcass:query(<<"update erlang_driver_test.entries1 set email = null where id = 'hello';">>),
    {ok, [{<<"email">>, text}], [[null]]} = erlcass:query(<<"select email from erlang_driver_test.entries1 where id = 'hello';">>).

async_insertion_roundtrip(_Config) ->
    Id = <<"hello_async">>,
    Age = 32,
    Email = <<"zz@test.com">>,

    {ok, Tag} = erlcass:query_async(<<"INSERT INTO erlang_driver_test.entries1(id, age, email) VALUES ('",
        Id/binary,"',",
        (integer_to_binary(Age))/binary, ", '",
        Email/binary, "')">>),

    receive
        {execute_statement_result, Tag, Result} ->
            ?assertEqual(Result, ok)

        after 10000 ->
            ct:fail("Timeout on executing query ~n", [])
    end,

    {ok, Tag2} = erlcass:query_async(<<"SELECT id, age, email FROM erlang_driver_test.entries1">>),

    receive
        {execute_statement_result, Tag2, Result2} ->
            Cols = [{<<"id">>, text},{<<"age">>, int}, {<<"email">>, text}],
            {ok, Cols, List} = Result2,
            ?assertEqual([Id, Age, Email], lists:last(List))

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
    Cols = datatypes_columns([ascii, bigint, blob, boolean, decimal, double, float, int, timestamp, uuid, varchar, varint, timeuuid, inet, tinyint, smallint, date, time]),
    SelectCols = [{<<"col1">>, ascii}, {<<"col2">>, bigint},    {<<"col3">>,  blob}, {<<"col4">>,  boolean}, {<<"col5">>,  decimal}, {<<"col6">>, double}, {<<"col7">>, float},
                  {<<"col8">>, int},  {<<"col9">>, timestamp}, {<<"col10">>, uuid}, {<<"col11">>, text}, {<<"col12">>, varint},    {<<"col13">>, timeuuid},  {<<"col14">>, inet},
                  {<<"col15">>,tinyint}, {<<"col16">>, smallint}, {<<"col17">>,date}, {<<"col18">>, time}],

    ok = erlcass:query(<<"CREATE TABLE erlang_driver_test.entries2(",  Cols/binary, " PRIMARY KEY(col1));">>),

    AsciiValBin = <<"hello">>,
    BigIntPositive = 9223372036854775807,
    Blob = <<1,2,3,4,5,6,7,8,9,10>>,
    BooleanTrue = true,
    DecimalPositive = {erlang:integer_to_binary(1234), 5},
    DoublePositive =  5.1235131241221e-6,
    FloatPositive = 5.12351e-6,
    TinyIntPositive = 127,
    SmallIntPositive = 32767,
    IntPositive = 2147483647,
    Timestamp = 2147483647,
    Date = 0,
    DateWithOffset = erlcass_time:date_from_epoch(Date),
    Time = 86399999999999,
    {ok, Uuid} = erlcass_uuid:gen_random(),
    Varchar1 = <<"Юникод"/utf8>>,
    Varint1 = erlang:integer_to_binary(1928301970128391280192830198049113123),
    {ok, Timeuuid} = erlcass_uuid:gen_time(),
    Inet = <<"127.0.0.1">>,

    InsertQuery = <<"
        INSERT INTO erlang_driver_test.entries2(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ">>,
    SelectQuery = <<"
        SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18
        FROM erlang_driver_test.entries2
        WHERE col1 = ?
    ">>,

    ok = erlcass:add_prepare_statement(insert_all_datatypes, InsertQuery),
    ok = erlcass:add_prepare_statement(select_all_datatypes, SelectQuery),

    ok = erlcass:execute(insert_all_datatypes, [
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
        Inet,
        TinyIntPositive,
        SmallIntPositive,
        DateWithOffset,
        Time
    ]),

    {ok, SelectCols, [Result]} = erlcass:execute(select_all_datatypes, [AsciiValBin]),
    [
        AsciiValBin,
        BigIntPositive,
        Blob,
        BooleanTrue,
        DecimalPositive,
        DoublePositive,
        _,
        IntPositive,
        Timestamp,
        Uuid,
        Varchar1,
        Varint1,
        Timeuuid,
        Inet,
        TinyIntPositive,
        SmallIntPositive,
        Date,
        Time
    ] = Result,

    AsciiString = "foo",
    BigIntNegative = -9223372036854775806,
    BooleanFalse = false,
    DecimalNegative = {erlang:integer_to_binary(1234), -5},
    DoubleNegative = -5.123513124122e-6,
    FloatNegative = -5.12351e-6,
    IntNegative = -2147483646,
    TinyIntNegative = -128,
    SmallIntNegative = -32768,
    Varchar2 = <<"åäö"/utf8>>,
    Varint2 = erlang:integer_to_binary(123124211928301970128391280192830198049113123),
    ok = erlcass:execute(insert_all_datatypes, ?BIND_BY_NAME, [
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
        {<<"col14">>, Inet},
        {<<"col15">>, TinyIntNegative},
        {<<"col16">>, SmallIntNegative},
        {<<"col17">>, DateWithOffset},
        {<<"col18">>, Time}
    ]),

    {ok, _, [Result2]} = erlcass:execute(select_all_datatypes, ?BIND_BY_NAME, [{<<"col1">>, AsciiString}]),
    BinAsciiString = list_to_binary(AsciiString),
    [
        BinAsciiString,
        BigIntNegative,
        Blob,
        BooleanFalse,
        DecimalNegative,
        DoubleNegative,
        _,
        IntNegative,
        Timestamp,
        Uuid,
        Varchar2,
        Varint2,
        Timeuuid,
        Inet,
        TinyIntNegative,
        SmallIntNegative,
        Date,
        Time
    ] = Result2.

async_custom_tag(_Config) ->
    CreationQ = <<"CREATE TABLE erlang_driver_test.async_custom_tag(key int PRIMARY KEY, value map<text, text>)">>,
    ok = erlcass:query(CreationQ),

    QuerySelect  = <<"SELECT value FROM erlang_driver_test.async_custom_tag where key = ?">>,
    Tag1 = {mytag, 1},

    ok = erlcass:add_prepare_statement(select_async_custom_tag, QuerySelect),
    ok = erlcass:async_execute(select_async_custom_tag, ?BIND_BY_INDEX, [1], self(), Tag1),

    receive
        {execute_statement_result, Tag1, Rs} ->
            Cols = [{<<"value">>, {map, text, text}}],
            {ok, Cols, []} = Rs,
            ok
    after 10000 ->
        ct:fail("Timeout on executing query ~n", [])
    end,
    ok.

prepared_bind_by_name_index(_Config) ->
    CreationQ = <<"CREATE TABLE erlang_driver_test.test_map(key int PRIMARY KEY, value map<text,text>)">>,
    ok = erlcass:query(CreationQ),

    CollectionIndex1 = <<"my_index">>,
    CollectionValue1 = <<"my_value">>,
    CollectionIndex2 = <<"my_index_2">>,
    CollectionValue2 = <<"my_value_2">>,
    Key1 = 5,
    Key2 = 10,
    QueryInsertDefaultConsistencyLevel  = <<"UPDATE erlang_driver_test.test_map SET value[?] = ? WHERE key = ?">>,
    QueryInsertLocalQuorum  = {<<"UPDATE erlang_driver_test.test_map SET value[?] = ? WHERE key = ?">>, ?CASS_CONSISTENCY_LOCAL_QUORUM},
    QueryInsertSerialConsistency  = {<<"UPDATE erlang_driver_test.test_map SET value[?] = ? WHERE key = ? IF EXISTS">>, [
        {serial_consistency_level, ?CASS_CONSISTENCY_LOCAL_SERIAL}
    ]},
    QuerySelect  = <<"SELECT value FROM erlang_driver_test.test_map where key = ?">>,

    ok = erlcass:add_prepare_statement(insert_test_bind, QueryInsertDefaultConsistencyLevel),
    ok = erlcass:add_prepare_statement(insert_test_bind_local_q, QueryInsertLocalQuorum),
    ok = erlcass:add_prepare_statement(insert_test_bind_serial_q, QueryInsertSerialConsistency),
    ok = erlcass:add_prepare_statement(select_test_bind, QuerySelect),

    ok = erlcass:execute(insert_test_bind, ?BIND_BY_NAME, [
        {<<"key(value)">>, CollectionIndex1},
        {<<"value(value)">>, CollectionValue1},
        {<<"key">>, Key1}
    ]),

    ok = erlcass:execute(insert_test_bind, [CollectionIndex2, CollectionValue2, Key2]),
    {ok, _, [[[{CollectionIndex1, CollectionValue1}]]]} = erlcass:execute(select_test_bind, ?BIND_BY_NAME, [{<<"key">>, Key1}]),
    {ok, _, [[[{CollectionIndex2, CollectionValue2}]]]} = erlcass:execute(select_test_bind, [Key2]),
    ok.

fire_and_forget(_Config) ->
    ok = erlcass:async_execute(insert_test_bind, ?BIND_BY_NAME, [
        {<<"key(value)">>, <<"my_index">>},
        {<<"value(value)">>, <<"my_value">>},
        {<<"key">>, 5}
    ], null, null),
    true.

collection_types(_Config) ->
    CreationQ = <<"CREATE TABLE erlang_driver_test.entries3(key varchar, numbers list<int>, names set<varchar>, phones map<int, varchar>, PRIMARY KEY(key));">>,
    ok = erlcass:query(CreationQ),

    Key2 = <<"somekeyhere_2">>,
    Key3 = <<"somekeyhere_3">>,
    List = [1,2,3,4,5,6,7,8,9,0],
    Set = [<<"item1">>, <<"item2">>, <<"item3">>],
    Map = [{100, <<"418-123-4545">>}, {200, <<"555-555-5555">>}],
    InsertQ = <<"INSERT INTO erlang_driver_test.entries3(key, numbers, names, phones) values (?, ?, ?, ?);">>,
    SelectQ = <<"SELECT key, numbers, names, phones FROM erlang_driver_test.entries3 WHERE key = ?;">>,
    SelectCols = [{<<"key">>, text}, {<<"numbers">>, {list, int}}, {<<"names">>, {set, text}}, {<<"phones">>, {map, int, text}}],
    ok = erlcass:add_prepare_statement(insert_collection_types, InsertQ),
    ok = erlcass:add_prepare_statement(select_collection_types, SelectQ),

    ok = erlcass:execute(insert_collection_types, ?BIND_BY_NAME, [
        {<<"key">>, Key2},
        {<<"numbers">>, List},
        {<<"names">>, Set},
        {<<"phones">>, Map}
    ]),

    ok = erlcass:execute(insert_collection_types, [Key3, List, Set, Map]),

    {ok, SelectCols, [[Key2, List, Set, Map]]} = erlcass:execute(select_collection_types, ?BIND_BY_NAME, [{<<"key">>, Key2}]),
    {ok, SelectCols, [[Key3, List, Set, Map]]} = erlcass:execute(select_collection_types, ?BIND_BY_INDEX, [Key3]),
    ok.

nested_collections(_Config) ->
    CreationQ = <<"CREATE TABLE erlang_driver_test.nested_collections(key varchar, numbers list<frozen<map<int, varchar>>>, PRIMARY KEY(key))">>,
    ok = erlcass:query(CreationQ),

    Key2 = <<"somekeyhere_2">>,
    Key3 = <<"somekeyhere_3">>,
    List = [[{100, <<"418-123-4545">>}, {200, <<"555-555-5555">>}], [{101, <<"1418-123">>}, {201, <<"5555-111">>}]],
    InsertQ = <<"INSERT INTO erlang_driver_test.nested_collections(key, numbers) values (?, ?)">>,
    SelectQ = <<"SELECT key, numbers FROM erlang_driver_test.nested_collections WHERE key = ?">>,
    SelectCols = [{<<"key">>, text}, {<<"numbers">>, {list, {map, int, text}}}],

    ok = erlcass:add_prepare_statement(nest_insert_collection_types, InsertQ),
    ok = erlcass:add_prepare_statement(nest_select_collection_types, SelectQ),

    ok = erlcass:execute(nest_insert_collection_types, ?BIND_BY_NAME, [{<<"key">>, Key2}, {<<"numbers">>, List}]),
    ok = erlcass:execute(nest_insert_collection_types, [Key3, List]),

    {ok, SelectCols, [[Key2, List]]} = erlcass:execute(nest_select_collection_types, ?BIND_BY_NAME, [{<<"key">>, Key2}]),
    {ok, SelectCols, [[Key3, List]]} = erlcass:execute(nest_select_collection_types, ?BIND_BY_INDEX, [Key3]),
    ok.

tuples(_Config) ->
    CreationQ = <<"CREATE TABLE erlang_driver_test.tuples (key varchar, item1 frozen<tuple<text, list<int>>>, item2 frozen< tuple< tuple<text, bigint> > >, PRIMARY KEY(key));">>,
    ok = erlcass:query(CreationQ),

    Key2 = <<"somekeyhere_2">>,
    Key3 = <<"somekeyhere_3">>,

    Item1 = {<<"sss">>, [1,2,3]},
    Item2 = {{<<"a">>, 1}},

    InsertQ = <<"INSERT INTO erlang_driver_test.tuples(key, item1, item2) values (?, ?, ?)">>,
    SelectQ = <<"SELECT key, item1, item2 FROM erlang_driver_test.tuples WHERE key = ?">>,
    SelectCols = [{<<"key">>, text}, {<<"item1">>, {tuple, [text, {list, int}]}}, {<<"item2">>, {tuple, [{tuple, [text, bigint]}]}}],

    ok = erlcass:add_prepare_statement(insert_tuple_types, InsertQ),
    ok = erlcass:add_prepare_statement(select_tuple_types, SelectQ),

    ok = erlcass:execute(insert_tuple_types, ?BIND_BY_NAME, [
        {<<"key">>, Key2},
        {<<"item1">>, Item1},
        {<<"item2">>, Item2}
    ]),

    ok = erlcass:execute(insert_tuple_types, [Key3, Item1, Item2]),

    {ok, SelectCols, [[Key2, Item1, Item2]]} = erlcass:execute(select_tuple_types, ?BIND_BY_NAME, [{<<"key">>, Key2}]),
    {ok, SelectCols, [[Key3, Item1, Item2]]} = erlcass:execute(select_tuple_types, ?BIND_BY_INDEX, [Key3]),
    ok.

test_udt(_Config) ->
    UserId = <<"62c36092-82a1-3a00-93d1-46196ee77204">>,

    Fname = <<"Marie-Claude">>,
    Lname = <<"Josset">>,

    Username = [
        {<<"firstname">>, Fname},
        {<<"lastname">>, Lname}
    ],

    Address = [
        {<<"street">>,<<"191 Rue St. Charles">>},
        {<<"city">>,<<"Paris">>},
        {<<"zip_code">>, 75015},
        {<<"phones">>, [<<"33 6 78 90 12 34">>]}
    ],

    ok = erlcass:query(<<"CREATE TYPE erlang_driver_test.address (street text, city text, zip_code int, phones set<text>)">>),
    ok = erlcass:query(<<"CREATE TYPE erlang_driver_test.fullname (firstname text, lastname text)">>),
    ok = erlcass:query(<<"CREATE TABLE erlang_driver_test.users (id uuid PRIMARY KEY, name frozen <fullname>, direct_reports set<frozen <fullname>>, addresses map<text, frozen <address>>)">>),

    ok = erlcass:add_prepare_statement(insert_udt, <<"INSERT INTO erlang_driver_test.users (id, name) VALUES (?, ?)">>),
    ok = erlcass:add_prepare_statement(update_address_udt, <<"UPDATE erlang_driver_test.users SET addresses = addresses + ? WHERE id=?;">>),
    ok = erlcass:add_prepare_statement(select_name_udt, <<"SELECT name FROM erlang_driver_test.users WHERE id=?">>),
    ok = erlcass:add_prepare_statement(select_last_name_udt, <<"SELECT name.lastname FROM erlang_driver_test.users WHERE id= ?">>),

    ok = erlcass:execute(insert_udt, [UserId, Username]),
    ok = erlcass:execute(update_address_udt, [[{<<"home">>, Address}], UserId]),

    {ok, _, [[Username]]} = erlcass:execute(select_name_udt, [UserId]),
    {ok, _, [[Lname]]} = erlcass:execute(select_last_name_udt, [UserId]).

batches(_Config) ->
    ok = erlcass:query(<<"TRUNCATE erlang_driver_test.entries1;">>),

    InsertStatement = <<"INSERT INTO erlang_driver_test.entries1(id, age, email) VALUES (?, ?, ?)">>,
    Id2 = <<"id_2">>,
    Age2 = 12,
    Email2 = <<"test2@test.com">>,

    {ok, Stm1} = erlcass:query_new_statement(<<"INSERT INTO erlang_driver_test.entries1(id, age, email) VALUES ('id_1', 11, 'test1@test.com')">>),
    ok = erlcass:add_prepare_statement(insert_prep, InsertStatement),

    {ok, Stm2} = erlcass:bind_prepared_statement(insert_prep),
    ok = erlcass:bind_prepared_params_by_name(Stm2, [{<<"id">>, Id2}, {<<"age">>, Age2}, {<<"email">>, Email2}]),

    ok = erlcass:batch_execute(?CASS_BATCH_TYPE_LOGGED, [Stm1, Stm2], [
        {consistency_level, ?CASS_CONSISTENCY_QUORUM},
        {serial_consistency_level, ?CASS_CONSISTENCY_LOCAL_SERIAL}
    ]),

    Cols = [{<<"id">>, text}, {<<"age">>, int}, {<<"email">>, text}],
    {ok, Cols, Result} = erlcass:query(<<"SELECT id, age, email FROM erlang_driver_test.entries1">>),
    ListLength = 2,
    ListLength = length(Result),
    ok.

erlcass_crash(_Config) ->
    exit(whereis(erlcass), kill),
    Key3 = <<"somekeyhere_3">>,
    timer:sleep(2000),
    {ok, _, _} = erlcass:execute(select_tuple_types, ?BIND_BY_INDEX, [Key3]),
    ok.

uuid_testing(_Config) ->
    Ts = 2147483647,
    {ok, _} = erlcass_uuid:gen_time(),
    {ok, _} = erlcass_uuid:gen_random(),
    {ok, UuidTs} = erlcass_uuid:gen_from_ts(Ts),
    {ok, Ts} = erlcass_uuid:get_ts(UuidTs),
    {ok, 1} = erlcass_uuid:get_version(UuidTs),
    {ok, _} = erlcass_uuid:max_from_ts(Ts),
    {ok, _} = erlcass_uuid:min_from_ts(Ts),
    ok.

date_time_testing(_Config) ->
    2147483648 = erlcass_time:date_from_epoch(0),
    2147483650 = erlcass_time:date_from_epoch(2 * 24 * 3600),

    Ts = 2147483647,
    Date = erlcass_time:date_from_epoch(Ts),
    Time = erlcass_time:time_from_epoch(Ts),
    Ts = erlcass_time:date_time_to_epoch(Date, Time).

get_metrics(_Config) ->
    {ok, _} = erlcass:get_metrics(),
    ok.

drop_keyspace(_Config) ->
    ok = erlcass:query(<<"DROP KEYSPACE erlang_driver_test">>).
