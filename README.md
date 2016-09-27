# ErlCass

*An Erlang Cassandra driver, based on [DataStax cpp driver][1] focused on performance.*

### Implementation note
 
#### How ErlCass affects the Erlang schedulers

It's well known that NIF's can affect the Erlang schedulers performances in case the functions are not returning in less than 1-2 ms and blocks the threads. 

Because the DataStax cpp driver is async, `ErlCass` won't block the scheduler threads and all calls to the native functions will return immediately.
The DataStax driver use it's own threads for managing the requests. Also the responses are received on this threads and sent back to Erlang calling process using `enif_send` in a async manner. 

#### Benchmark comparing with other drivers

The benchmark (`test/benchmark.erl`) is spawning N processes that will run a total of X request using the async api's and then waits to read X responses.
In `test/test.config` you can find the config's for every application. During test in case of unexpected results from driver will log errors in console.

Notes:

- `marina` is currently disabled. Seems is not compiling with rebar on OSX because of one of it's deps.
- Test was run on MacBook Pro with OSX El Capitan
- The schema was created using `load_test:prepare_load_test_table` from `test/load_test.erl`. Basically the schema contains all possible 
data types and the query is based on a primary key (will return the same row all the time which is fine because we test the driver performances and not the server one)
 
```erlang
benchmark:run(Module, NrProcesses, NrReq).
```

Used 100 concurrent processes that sends 100k queries. Measured the average time for 3 runs:
 
| cassandra driver   | Time to complete (ms) | Req/sec  |
|:------------------:|:---------------------:|:--------:|
| erlcass v2.5       | 2131                  | 46926    | 
| cqerl v1.0.1       | 4544                  | 22007    |

#### Changelog

Changelog is available [here][5].

### Getting started:

The application is compatible with both `rebar` or `rebar3`.

In case you receive any error related to compiling of the DataStax driver you can try to run `rebar` with `sudo` in order 
to install all dependencies. Also you can check [wiki section][2] for more details

### Data types

In order to see the relation between Cassandra column types and Erlang types please check this [wiki section][3]

### Starting the application

```erlang
application:start(erlcass).
```

### Enable logs and setting custom log handler

Available Log levels are:

```erlang
-define(CASS_LOG_DISABLED, 0).
-define(CASS_LOG_CRITICAL, 1).
-define(CASS_LOG_ERROR, 2).
-define(CASS_LOG_WARN, 3). (default)
-define(CASS_LOG_INFO, 4).
-define(CASS_LOG_DEBUG,5).
-define(CASS_LOG_TRACE, 6).
```

In order to change the log level for the native driver you need to set the `log_level` environment variable for ErlCass into your config file.
By default the logs are printed to console. In order to print them into an external log system you can use the `set_log_function` method.
The callback should be a function with arity 1 which will receive a record of `log_msg` type defined as

`
-record(log_msg, {ts, severity, severity_str, file, line, function, message}).
`

where

- `ts` is The millisecond timestamp (since the Epoch) when the message was logged (int)
- `severity` The severity of the log message (int value from 1 to 6)
- `severity_str` The severity of the log message as a string value (binary string)
- `file` The file where the message was logged (binary string)
- `line` The line in the file where the message was logged (int)
- `function` The function where the message was logged (binary string)
- `message` The message (binary string)

or under `{_Severity, Msg, Args}` format (for all messages generated from Erlang code)

### Setting the cluster options

The cluster options can be set at runtime using `erlcass:set_cluster_options/1` method as follow:

```erlang
ok = erlcass:set_cluster_options([
    {contact_points, <<"172.17.3.129,172.17.3.130,172.17.3.131">>},
    {port, 9042},
    ...
]).
```

or inside your `app.config` file:

```erlang
{erlcass, [
    {log_level, 3},
    {keyspace, <<"keyspace">>},
    {cluster_options,[
        {contact_points, <<"172.17.3.129,172.17.3.130,172.17.3.131">>},
        {port, 9042},
        {load_balance_dc_aware, {<<"dc-name">>, 0, false}},
        {latency_aware_routing, true},
        {token_aware_routing, true},
        {number_threads_io, 4},
        {queue_size_io, 128000},
        {max_connections_host, 5},
        {pending_requests_high_watermark, 128000},
        {tcp_nodelay, true},
        {tcp_keepalive, {true, 1800}},
        {default_consistency_level, 6}
    ]}
]},
```

Tips for production environment:

- Use `token_aware_routing` and `latency_aware_routing`
- Don't use `number_threads_io` bigger than the number of your cores.
- Use `tcp_nodelay` and also enable `tcp_keepalive` 

All available options are described in the following [wiki section][4].

### Creating a session

*Currently this is limited to one session per application. This is a DataStax recommendations as well*

In order to connect the session to a keyspace as well use as option:

```erlang
 [{keyspace, <<"keyspace_name_here">>}].
```
In case you don't want to connect the session to any keyspace use as argument an empty list.

Example:

```erlang
ok = erlcass:create_session([{keyspace, <<"stresscql">>}]).
```

### Add a prepare statement

Example:

```erlang
ok = erlcass:add_prepare_statement(select_blogpost,
                                   <<"select * from blogposts where domain = ? LIMIT 1">>),
```

In case you want to overwrite the default consistency level for that prepare statement use a tuple for the query argument: `{Query, ConsistencyLevelHere}`

Also this is possible using `{Query, Options}` where options is a proplist with the following options supported:
                                                                           
- `consistency_level` - If it's missing the statement will be executed using the default consistency level value.
- `serial_consistency_level` - That consistency can only be either `?CASS_CONSISTENCY_SERIAL` or `?CASS_CONSISTENCY_LOCAL_SERIAL` and if not present, it defaults to `?CASS_CONSISTENCY_SERIAL`. This option will be ignored for anything else that a conditional update/insert.

Example:

```erlang
ok = erlcass:add_prepare_statement(
                select_blogpost,
                { <<"select * from blogposts where domain = ? LIMIT 1">>, ?CASS_CONSISTENCY_LOCAL_QUORUM }).
```

or 

```erlang
ok = erlcass:add_prepare_statement(
                insert_blogpost,
                {<<"UPDATE blogposts SET author = ? WHERE domain = ? IF EXISTS">>, [
                    {consistency_level, ?CASS_CONSISTENCY_LOCAL_QUORUM},
                    {serial_consistency_level, ?CASS_CONSISTENCY_LOCAL_SERIAL}
                ]}).
```

### Run a prepared statement query

In case the first parameter for `erlcass:execute` is an atom then the driver will try to find the associated prepared statement and to run it.
You can bind the parameters in 2 ways: by name and by index. You can use `?BIND_BY_INDEX` and `?BIND_BY_NAME` from execute/3 in order to specify the desired method. By default is binding by index

Example:

```erlang
%bind by name
erlcass:execute(select_blogpost, ?BIND_BY_NAME, [{<<"domain">>, <<"Domain_1">>}]).
%bind by index
erlcass:execute(select_blogpost, [<<"Domain_1">>]).
%bind by index
erlcass:execute(select_blogpost, ?BIND_BY_INDEX, [<<"Domain_1">>]).
```

In case of maps you can use `key(field)` and `value(field)` in order to bind by name.

```erlang
%table: CREATE TABLE test_map(key int PRIMARY KEY, value map<text,text>)
%statement: UPDATE examples.test_map SET value[?] = ? WHERE key = ?
%bind by index
erlcass:execute(identifier, [<<"collection_key_here">>, <<"collection_value_here">>, <<"key_here">>]).
%bind by name
erlcass:execute(insert_test_bind, ?BIND_BY_NAME, [{<<"key(value)">>, CollectionIndex1}, {<<"value(value)">>, CollectionValue1}, {<<"key">>, Key1}]),
```

### Async queries and blocking queries

For blocking operations use `erlcass:execute`, for async execution use : `erlcass:async_execute`.
The blocking operation will block the current erlang process (still async into the native code in order to avoid freezing of the VM threads) until will get the result from the cluster.

In case of an async execution the calling process will receive a message of the following form: `{execute_statement_result, Tag, Result}`

For example:

```erlang
{ok, Tag} = erlcass:async_execute(...),
    receive
        {execute_statement_result, Tag, Result} ->
            Result
    end.
```

### Non prepared statements queries

The only downside is that you have to provide metadata about the types of the fields that are bound.
The data types can be found into `erlcass.hrl` file as follow:

```erlang
-define(CASS_TEXT, text).                         %use for (ascii, text, varchar)
-define(CASS_TINYINT, tinyint).                   %use for (tinyint)
-define(CASS_SMALLINT, smallint).                 %use for (smallint)
-define(CASS_INT, int).                           %use for (int)
-define(CASS_DATE, date).                         %use for (date)
-define(CASS_BIGINT, bigint).                     %use for (timestamp, counter, bigint, time)
-define(CASS_BLOB, blob).                         %use for (varint, blob)
-define(CASS_BOOLEAN, bool).                      %use for (bool)
-define(CASS_FLOAT, float).                       %use for (float)
-define(CASS_DOUBLE, double).                     %use for (double)
-define(CASS_INET, inet).                         %use for (inet)
-define(CASS_UUID, uuid).                         %use for (timeuuid, uuid)
-define(CASS_DECIMAL, decimal).                   %use for (decimal)
-define(CASS_LIST(ValueType), {list, ValueType}). %use for list
-define(CASS_SET(ValueType), {set, ValueType}).   %use for set
-define(CASS_MAP(KeyType, ValueType), {map, KeyType, ValueType}). %use for map
-define(CASS_TUPLE(Types), {tuple, Types}).       %use for tuples
```

The same rules apply for setting the desired consistency level as on prepared statements (see Add prepare statement section).
Example with binding by index (requires metadata parsing all the time so it might not be the best solution when using non prepared statements):

```erlang
erlcass:execute(<<"select * from blogposts where domain = ? LIMIT 1">>,
                [{?CASS_TEXT, <<"Domain_1">>}]).
```
or:

```erlang
erlcass:execute(<<"select * from blogposts where domain = 'Domain_1' LIMIT 1">>).
```

### Batched queries

In order to perform batched statements you can use `erlcass:batch_async_execute/3` or `erlcass:batch_execute/3`.

First argument is the batch type and is defined as:

```erlang
-define(CASS_BATCH_TYPE_LOGGED, 0).
-define(CASS_BATCH_TYPE_UNLOGGED, 1).
-define(CASS_BATCH_TYPE_COUNTER, 2).
```

The second one is a list of statements (prepared or normal statements) that needs to be executed in the batch.

The third argument is a list of options in `{Key, Value}` format (proplist):

- `consistency_level` - If it's missing the batch will be executed using the default consistency level value.
- `serial_consistency_level` - That consistency can only be either `?CASS_CONSISTENCY_SERIAL` or `?CASS_CONSISTENCY_LOCAL_SERIAL` and if not present, it defaults to `?CASS_CONSISTENCY_SERIAL`. This option will be ignored for anything else that a conditional update/insert.

Example:

```erlang
InsertStatement = <<"INSERT INTO erlang_driver_test.entries1(id, age, email) VALUES (?, ?, ?)">>,
ok = erlcass:add_prepare_statement(insert_prep, InsertStatement),
{ok, Stm1} = erlcass:create_statement(InsertStatement, [{?CASS_TEXT, Id1}, {?CASS_INT, Age1}, {?CASS_TEXT, Email1}]),
{ok, Stm2} = erlcass:bind_prepared_statement(insert_prep),
ok = erlcass:bind_prepared_params_by_name(Stm2, [{<<"id">>, Id2}, {<<"age">>, Age2}, {<<"email">>, Email2}]),
{ok, []} = erlcass:batch_execute(?CASS_BATCH_TYPE_LOGGED, [Stm1, Stm2], [{consistency_level, ?CASS_CONSISTENCY_QUORUM}]).
```

### Working with uuid or timeuuid fields:

- `erlcass_uuid:gen_time()`   -> Generates a V1 (time) UUID
- `erlcass_uuid:gen_random()` -> Generates a new V4 (random) UUID
- `erlcass_uuid:gen_from_ts(Ts)` -> Generates a V1 (time) UUID for the specified timestamp
- `erlcass_uuid:min_from_ts(Ts)` -> Sets the UUID to the minimum V1 (time) value for the specified timestamp,
- `erlcass_uuid:max_from_ts(Ts)` -> Sets the UUID to the maximum V1 (time) value for the specified timestamp,
- `erlcass_uuid:get_ts(Uuid)` -> Gets the timestamp for a V1 UUID,
- `erlcass_uuid:get_version(Uuid)` -> Gets the version for a UUID (V1 or V4)

### Working with date, time fields:

- `erlcass_time:date_from_epoch(EpochSecs)` -> Converts a unix timestamp (in seconds) to the Cassandra `date` type. The `date` type represents the number of days since the Epoch (1970-01-01) with the Epoch centered at the value 2^31.
- `erlcass_time:time_from_epoch(EpochSecs)` -> Converts a unix timestamp (in seconds) to the Cassandra `time` type. The `time` type represents the number of nanoseconds since midnight (range 0 to 86399999999999).
- `erlcass_time:date_time_to_epoch(Date, Time)` -> Combines the Cassandra `date` and `time` types to Epoch time in seconds. Returns Epoch time in seconds. Negative times are possible if the date occurs before the Epoch (1970-1-1).

### Getting metrics

In order to get metrics from the native driver you can use `erlcass:get_metrics().`

##### requests

- `min` - Minimum in microseconds
- `max` - Maximum in microseconds
- `mean` - Mean in microseconds
- `stddev` - Standard deviation in microseconds
- `median` - Median in microseconds
- `percentile_75th` - 75th percentile in microseconds
- `percentile_95th` - 95th percentile in microseconds
- `percentile_98th` - 98th percentile in microseconds
- `percentile_99th` - 99the percentile in microseconds
- `percentile_999th` - 99.9th percentile in microseconds
- `mean_rate` - Mean rate in requests per second
- `one_minute_rate` - 1 minute rate in requests per second
- `five_minute_rate` - 5 minute rate in requests per second
- `fifteen_minute_rate` - 15 minute rate in requests per second

##### stats

- `total_connections` - The total number of connections
- `available_connections` - The number of connections available to take requests
- `exceeded_pending_requests_water_mark` - Occurrences when requests exceeded a pool's water mark
- `exceeded_write_bytes_water_mark` - Occurrences when number of bytes exceeded a connection's water mark

##### errors

- `connection_timeouts` - Occurrences of a connection timeout
- `pending_request_timeouts` - Occurrences of requests that timed out waiting for a connection
- `request_timeouts` - Occurrences of requests that timed out waiting for a request to finish

### Low level methods

Each query requires an internal statement (prepared or not). You can reuse the same statement object for multiple queries
performed in the same process.

##### Getting a statement reference for a prepared statement query

```erlang
{ok, Statement} = erlcass:bind_prepared_statement(select_blogpost).
```

##### Getting a statement reference for a non prepared query

```erlang
{ok, Statement} = erlcass:create_statement(<<"select * from blogposts where domain = ? LIMIT 1">>,
                                           [{?CASS_TEXT, <<"Domain_1">>}]).
```

##### Bind the values for a prepared statement before executing

```erlang
%bind by name
ok = erlcass:bind_prepared_params_by_name(select_blogpost, [{<<"domain">>, <<"Domain_1">>}]);
%bind by index
ok = erlcass:bind_prepared_params_by_index(select_blogpost, [<<"Domain_1">>]);
```

For mode details about bind by index and name please see: 'Run a prepared statement query' section

##### Execute a statement async

```erlang
{ok, Tag} = erlcass:async_execute_statement(Statement).
```

##### Execute a statement in blocking mode

```erlang
Result = erlcass:execute_statement(Statement).
```

Using this low level functions are very useful when you want to run in loop a certain query. Helps you to avoid recreating the statements all the time.
For example here is how the execute method is implemented:

```erlang
execute(Identifier, Params) ->
    if
        is_atom(Identifier) ->
            {ok, Statement} = bind_prepared_statement(Identifier),
            ok = bind_prepared_params(Statement, Params);
        true ->
            {ok, Statement} = create_statement(Identifier, Params)
    end,
    execute_statement(Statement).
```

[1]:https://github.com/datastax/cpp-driver
[2]:https://github.com/silviucpp/erlcass/wiki/Getting-started
[3]:https://github.com/silviucpp/erlcass/wiki/Data-types
[4]:https://github.com/silviucpp/erlcass/wiki/Available-cluster-options
[5]:https://github.com/silviucpp/erlcass/blob/master/CHANGELOG.md