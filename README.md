# ErlCass

[![Build Status](https://travis-ci.org/silviucpp/erlcass.svg?branch=master)](https://travis-ci.org/silviucpp/erlcass)
[![GitHub](https://img.shields.io/github/license/silviucpp/erlcass)](https://github.com/silviucpp/erlcass/blob/master/LICENSE)
[![Hex.pm](https://img.shields.io/hexpm/v/erlcass)](https://hex.pm/packages/erlcass)
[![Maintenance](https://img.shields.io/maintenance/yes/2019)]()

*An Erlang Cassandra driver, based on [DataStax cpp driver][1] focused on performance.*

### Note for v4.0.0

- Starting with `erlcass` version v4.x the native driver is based on Datastax cpp-driver > 2.10.0 which is a massive 
release that includes many new features as well as architectural and performance improvements. 

- Some cluster configs were removed and some other were added. For more info please see the [Changelog][5].
- This new version adds support for speculative execution: For certain applications it is of the utmost importance to 
minimize latency. Speculative execution is a way to minimize latency by preemptively executing several instances of 
the same query against different nodes. The fastest response is then returned to the client application and the other 
requests are cancelled. Speculative execution is disabled by default. (see `speculative_execution_policy`)

### Update from 2.x to 3.0

This update breaks the compatibility with the other versions. All query results will return in case of success:
- `ok` instead `{ok, []}` for all DDL and DML queries (because they never returns any column or row)
- `{ok, Columns, Rows}` instead `{ok, Rows}`, where also each row is returned as a list not as a tuple as was before.

### Implementation note

#### How ErlCass affects the Erlang schedulers

It's well known that NIF's can affect the Erlang schedulers performances in case the functions are not returning in less
than 1-2 ms and blocks the threads.

Because the DataStax cpp driver is async, `ErlCass` won't block the scheduler threads and all calls to the native
functions will return immediately. The DataStax driver use it's own thread pool for managing the requests.
Also the responses are received on this threads and sent back to Erlang calling processes using `enif_send` in
an async manner.

#### Features

List of supported features:

- Asynchronous API
- Synchronous API
- Simple, Prepared, and Batch statements
- Asynchronous I/O, parallel execution, and request pipelining
- Connection pooling
- Automatic node discovery
- Automatic reconnection
- Configurable load balancing
- Works with any cluster size
- Authentication
- SSL
- Latency-aware routing
- Performance metrics
- Tuples and UDTs
- Nested collections
- Retry policies
- Support for materialized view and secondary index metadata
- Support for clustering key order, `frozen<>` and Cassandra version metadata
- Reverse DNS with SSL peer identity verification support
- Randomized contact points
- Speculative execution

Missing features from Datastax driver can be found into the [Todo List][9].

#### Benchmark comparing with other drivers

The benchmark (`benchmarks/benchmark.erl`) is spawning N processes that will send a total of X request using the async
api's and then waits to read X responses. In `benchmarks/benchmark.config` you can find the config's for every driver
used in tests. During test in case of unexpected results from driver will log errors in console.

To run the benchmark yourself you should do:

- change the cluster ip in `benchmark.config` for all drivers
- run `make setup_benchmark` (this will compile the app using the bench profile and create the necessary schema)
- use `make benchmark` as described above

The following test was run on a Ubuntu 16.04 LTS (Intel(R) Core(TM) i5-2500 CPU @ 3.30GHz 4 cores) and the cassandra cluster was running on other 3
physical machines in the same LAN. The schema is created using `prepare_load_test_table` from `benchmarks/load_test.erl`.
Basically the schema contains all possible data types and the query is based on a primary key (will return the same
row all the time which is fine because we test the driver performances and not the server one)

To create schema:

```erlang
make setup_benchmark
```

To run the benchmark:

```erlang
make benchmark MODULE=erlcass PROCS=100 REQ=100000
```

Where:

- `MODULE`: the driver used to benchmark. Can be one of : `erlcass` or `marina`
- `PROCS`: the number or erlang processes used to send the requests (concurrency level). Default 100.
- `REQ`: the number of requests to be sent. Default 100000.

The results for 100 concurrent processes that sends 100k queries. Picked the average time from 3 runs:

| cassandra driver     | Time (ms) | Req/sec  |
|:--------------------:| ---------:|---------:|
| [erlcass][8] v4.0.0    | 947     | 105544   |
| [marina][7] 0.3.5    | 2360      | 42369    |


#### Changelog

Changelog is available [here][5].

### Getting started:

The application is compatible with both `rebar` or `rebar3`.

In case you receive any error related to compiling of the DataStax driver you can try to run `rebar` with `sudo` in
order to install all dependencies. Also you can check [wiki section][2] for more details

### Data types

In order to see the relation between Cassandra column types and Erlang types please check this [wiki section][3]

### Starting the application

```erlang
application:start(erlcass).
```

### Setting the log level

`Erlcass` is using `lager` for logging the errors. Beside the fact that you can set in lager the desired log level,
for better performances it's better to set also in `erlcass` the desired level otherwise there will be a lot of
resources consumed by lager to format the messages and then drop them. Also the native driver performances can be
affected because of the time spent in generating the logs and sending them from C++ into Erlang.  

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

In order to change the log level for the native driver you need to set the `log_level` environment variable for
`erlcass` into your app config file, example: `{log_level, 3}`.

### Setting the cluster options

The cluster options can be set inside your `app.config` file under the `cluster_options` key:

```erlang
{erlcass, [
    {log_level, 3},
    {keyspace, <<"keyspace">>},
    {cluster_options,[
        {contact_points, <<"172.17.3.129,172.17.3.130,172.17.3.131">>},       
        {latency_aware_routing, true},
        {token_aware_routing, true},
        {number_threads_io, 4},
        {queue_size_io, 128000},
        {core_connections_host, 1},
        {tcp_nodelay, true},
        {tcp_keepalive, {true, 60}},
        {connect_timeout, 5000},
        {request_timeout, 5000},
        {retry_policy, {default, true}},
        {default_consistency_level, 6}
    ]}
]},
```

### Tips for production environment:

- Use `token_aware_routing` and `latency_aware_routing`
- Don't use `number_threads_io` bigger than the number of your cores.
- Use `tcp_nodelay` and also enable `tcp_keepalive`
- Don't use large values for `core_connections_host`. The driver is system call bound and performs better with less I/O threads 
and connections because it can batch a larger number of writes into a single system call (the driver will naturally attempt to coallesce these operations). 
You may want to reduce the number of I/O threads to 2 or 3 and reduce the core connections to 1 (default).

All available options are described in the following [wiki section][4].

### Add a prepare statement

Example:

```erlang
ok = erlcass:add_prepare_statement(select_blogpost,
                                   <<"select * from blogposts where domain = ? LIMIT 1">>),
```

In case you want to overwrite the default consistency level for that prepare statement use a tuple for the
query argument: `{Query, ConsistencyLevelHere}`

Also this is possible using `{Query, Options}` where options is a proplist with the following options supported:

- `consistency_level` - If it's missing the statement will be executed using the default consistency level value.
- `serial_consistency_level` - This consistency can only be either `?CASS_CONSISTENCY_SERIAL` or
`?CASS_CONSISTENCY_LOCAL_SERIAL` and if not present, it defaults to `?CASS_CONSISTENCY_SERIAL`. This option will be
ignored for anything else that a conditional update/insert.

Example:

```erlang
ok = erlcass:add_prepare_statement(select_blogpost,
        {<<"select * from blogposts where domain = ? LIMIT 1">>, ?CASS_CONSISTENCY_LOCAL_QUORUM}).
```

or

```erlang
ok = erlcass:add_prepare_statement(insert_blogpost, {
        <<"UPDATE blogposts SET author = ? WHERE domain = ? IF EXISTS">>, [
        {consistency_level, ?CASS_CONSISTENCY_LOCAL_QUORUM},
        {serial_consistency_level, ?CASS_CONSISTENCY_LOCAL_SERIAL}]
}).
```

### Run a prepared statement query

You can bind the parameters in 2 ways: by name and by index. You can use `?BIND_BY_INDEX` and `?BIND_BY_NAME` from
`execute/3` in order to specify the desired method. By default is binding by index

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

erlcass:execute(insert_test_bind, ?BIND_BY_NAME, [
    {<<"key(value)">>, CollectionIndex1},
    {<<"value(value)">>, CollectionValue1},
    {<<"key">>, Key1}
]),
```

### Async queries and blocking queries

For blocking operations use `erlcass:execute`, for async execution use : `erlcass:async_execute`.

The blocking operation the calling process will block (still async into the native code in order to avoid
freezing of the VM threads) until will get the result from the cluster.

In case of an async execution the calling process will receive a message of the following format:
`{execute_statement_result, Tag, Result}` when the data from the server was retrieved.

For example:

```erlang
{ok, Tag} = erlcass:async_execute(...),
    receive
        {execute_statement_result, Tag, Result} ->
            Result
    end.
```

### Non prepared statements queries

In order to run queries that you don't want to run them as prepared statements you can use:
`query/1`, `query_async/1` or `query_new_statement/1` (in order to create a query statement that can be executed into a
batch query along other prepared or not prepared statements)

The same rules apply for setting the desired consistency level as on prepared statements (see Add prepare statement
section).

```erlang
erlcass:query(<<"select * from blogposts where domain = 'Domain_1' LIMIT 1">>).
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
- `serial_consistency_level` - That consistency can only be either `?CASS_CONSISTENCY_SERIAL` or
`?CASS_CONSISTENCY_LOCAL_SERIAL` and if not present, it defaults to `?CASS_CONSISTENCY_SERIAL`. This option will be
ignored for anything else that a conditional update/insert.

Example:

```erlang
ok = erlcass:add_prepare_statement(insert_prep, <<"INSERT INTO table1(id, age, email) VALUES (?, ?, ?)">>),

{ok, Stm1} = erlcass:query_new_statement(<<"UPDATE table2 set foo = 'bar'">>),

{ok, Stm2} = erlcass:bind_prepared_statement(insert_prep),
ok = erlcass:bind_prepared_params_by_index(Stm2, [Id2, Age2, Email2]),

ok = erlcass:batch_execute(?CASS_BATCH_TYPE_LOGGED, [Stm1, Stm2], [
    {consistency_level, ?CASS_CONSISTENCY_QUORUM}
]).
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

- `erlcass_time:date_from_epoch(EpochSecs)` -> Converts a unix timestamp (in seconds) to the Cassandra `date` type.
The `date` type represents the number of days since the Epoch (1970-01-01) with the Epoch centered at the value 2^31.
- `erlcass_time:time_from_epoch(EpochSecs)` -> Converts a unix timestamp (in seconds) to the Cassandra `time` type.
The `time` type represents the number of nanoseconds since midnight (range 0 to 86399999999999).
- `erlcass_time:date_time_to_epoch(Date, Time)` -> Combines the Cassandra `date` and `time` types to Epoch time
in seconds. Returns Epoch time in seconds. Negative times are possible if the date occurs before the Epoch (1970-1-1).

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

##### errors

- `connection_timeouts` - Occurrences of a connection timeout
- `pending_request_timeouts` - Occurrences of requests that timed out waiting for a connection
- `request_timeouts` - Occurrences of requests that timed out waiting for a request to finish

### Low level methods

Each query requires an internal statement (prepared or not). You can reuse the same statement object for multiple
queries performed in the same process.

##### Getting a statement reference for a prepared statement query

```erlang
{ok, Statement} = erlcass:bind_prepared_statement(select_blogpost).
```

##### Getting a statement reference for a non prepared query

```erlang
{ok, Statement} = erlcass:query_new_statement(<<"select * from blogposts where domain = 'Domain_1' LIMIT 1">>).
```

##### Bind the values for a prepared statement before executing

```erlang
%bind by name
ok = erlcass:bind_prepared_params_by_name(select_blogpost, [{<<"domain">>, <<"Domain_1">>}]);

%bind by index
ok = erlcass:bind_prepared_params_by_index(select_blogpost, [<<"Domain_1">>]);
```

For mode details about bind by index and name please see: 'Run a prepared statement query' section

[1]:https://github.com/datastax/cpp-driver
[2]:https://github.com/silviucpp/erlcass/wiki/Getting-started
[3]:https://github.com/silviucpp/erlcass/wiki/Data-types
[4]:https://github.com/silviucpp/erlcass/wiki/Available-cluster-options
[5]:https://github.com/silviucpp/erlcass/blob/master/CHANGELOG.md
[6]:https://github.com/matehat/cqerl
[7]:https://github.com/lpgauth/marina
[8]:https://github.com/silviucpp/erlcass
[9]:https://github.com/silviucpp/erlcass/wiki/Todo-list
