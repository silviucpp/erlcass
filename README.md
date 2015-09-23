# ErlCass

*An Erlang Cassandra driver, based on Datastax cpp driver focused on performance.*

#####The project is under development.

### TODO List:

- Add support for Setting serial consistency,
- Add support for pagination,
- Add support for setting log level and custom handler
- Add support for UDT
- Add support for Cassandra 2.2 data types `tinyint` and `smallint`
- Add support for the Cassandra 2.2 `date` data type
- Add support for functions to convert from Unix Epoch time (in seconds) to and from the Cassandra `date` and `time` types
- Add support for sending and relieving custom payloads to and from Cassandra
- Add support for server-side warnings
- Add support for retry policies settings
- Add support for client-side timestamps settings

### Changelog:

##### v2.0

- Added support for tuples
- Added support for nested collections
- Based on cpp driver 2.2.0-beta1
- Interfaces changes: `bind_prepared_params` replaced by `bind_prepared_params_by_name` and `bind_prepared_params_by_index`
- Add support for `async_execute/1` and `execute/1` (should be used when no binding params available)
- Add support for `async_execute/3` and `execute/3` (second parameter should be used to specify the binding type - by name or index)
- By default `async_execute/2` and `execute/2` are binding the params by index

##### v1.0

- Initial implementation supporting most of basic features available in Cpp Driver
- Based on cpp driver 1.0.3

### Getting starting:

*On the most recent driver version the rebar script will install for you all dependencies on Cent OS, Ubuntu and Mac OS. So you can skip this section.*

Make sure you have all dependencies required to compile the Datastax C++ Driver.
More details [here][1].

For example:

Centos:

```sh
sudo yum install automake cmake gcc-c++ git libtool openssl-devel wget
pushd /tmp
wget http://libuv.org/dist/v1.4.2/libuv-v1.4.2.tar.gz
tar xzf libuv-v1.4.2.tar.gz
pushd libuv-v1.4.2
sh autogen.sh
./configure
sudo make install
popd
popd
```

Also on CentOS you need to add `﻿usr/local/lib` into `ldconfig` search path.

```sh
su -
﻿nano /etc/ld.so.conf.d/usrlocal.conf
﻿add inside the file: usr/local/lib
﻿ldconfig -v
```

Ubuntu:

```sh
sudo apt-add-repository ppa:linuxjedi/ppa
sudo apt-get update
sudo apt-get install g++ make cmake libuv-dev libssl-dev
```

Mac OS:

```sh
brew install libuv cmake
```

### Data types

Here is a correspondence of Cassandra column types with their equivalent Erlang types

Cassandra Column Type      | Erlang types                                 | Examples
-------------------------- | -------------------------------------------- | :----------------------------:
ascii                      | binary or string                             | <<"hello">> or "hello"
varchar                    | binary or string                             | <<"hello">> or "hello"
text                       | binary or string                             | <<"hello">> or "hello"
bigint                     | integer (signed 64-bit)                      | 9223372036854775807
timestamp                  | integer (signed 64-bit)                      | 9223372036854775807
counter                    | integer (signed 64-bit)                      | 9223372036854775807
blob                       | binary                                       | <<1,2,3,4,5,6,7,8,9,10>>
varint                     | binary                                       | <<"12423423423423423423243432432">>
boolean                    | `true`, `false`                              | true
decimal                    | `{Unscaled :: binary(), Scale :: integer()}` | {<<"1234">>, 5}
double                     | float (signed 64-bit)                        | 5.1235131241221e-6
float                      | float (signed 32-bit)                        | 5.12351e-6
int                        | integer (signed 32-bit)                      | 2147483647
uuid                       | binary                                       | <<"61c16fb1-44ca-4591-9317-ac96ddbd8694">>
varint                     | binary                                       | <<"1928301970128391280192830198049113123">>
timeuuid                   | binary                                       | <<"076a46c0-0ad7-11e5-b314-3d7bf89b87a1">>
inet                       | binary                                       | <<"127.0.0.1">>
tuple                      | erlang tuple                                 | {<<"aaa">>, 1}

In order to generate a uuid v4 you can use `erlcass:uuid_gen_random()` for uuid v1 you can use `erlcass:uuid_gen_time()`.
For more details please see the section dedicated to uuid's

### Starting the application

```erlang
application:start(erlcass).
```

### Setting the cluster options

```erlang
ok = erlcass:set_cluster_options([
            {contact_points,<<"172.17.3.129">>},
            {load_balance_round_robin, true},
            {token_aware_routing, true},
            {load_balance_dc_aware, {<<"dc-beta">>, 0, false}},
            {default_consistency_level, ?CASS_CONSISTENCY_ONE},
            {number_threads_io, 4},
            {queue_size_io, 124000},
            {core_connections_host, 5},
            {max_connections_host, 5},
            {tcp_nodelay, true},
            {tcp_keepalive, {true, 60}},
            {pending_requests_high_watermark, 128000}
]).
```

Available options:

##### contact_points (Mandatory)

Example : {contact_points, <<"172.17.3.129">>}

Sets/Appends contact points. The first call sets the contact points and any subsequent calls appends additional contact points.
Passing an empty string will clear the contact points. White space is striped from the contact points.
Accepted values: <<"127.0.0.1">> <<"127.0.0.1,127.0.0.2">>, <<"server1.domain.com">>

##### port

Example: {port, 9042}

Sets the port.

Default: 9042

##### ssl

Example:

```erlang
{ssl, [
            {trusted_certs, [<<"cert1">>, <<"cert2">>]},
            {cert, <<"cert_here">>},
            {private_key, {<<"private_key_here">>, <<"private_key_pwd_here">>}},
            {verify_flags, ?CASS_SSL_VERIFY_PEER_CERT}
        ]
}
```

Sets the SSL context and enables SSL.

Default: None

###### Params:

```erlang
{ssl, [
        {trusted_certs, CertsList::list()},
        {cert, Cert::binary()},
        {private_key, {PrivateKey::binary(), KeyPassword::binary()}},
        {verify_flags, VerifyFlags::integer()}
       ]
}
```

- `trusted_certs` : Adds one or more trusted certificate. This is used to verify the peer's certificate.
- `cert` : Set client-side certificate chain. This is used to authenticate the client on the server-side. This should contain the entire Certificate chain starting with the certificate itself.
- `private_key` : Set client-side private key. This is used to authenticate the client on the server-side. PrivateKey is a key PEM formatted key string and KeyPassword is the password used to decrypt key
- `verify_flags` : Sets verification performed on the peer's certificate.

For verify_flags use one of the values defined in `erlcass.hrl` :

```erlang
-define(CASS_SSL_VERIFY_NONE, 0).
-define(CASS_SSL_VERIFY_PEER_CERT, 1).
-define(CASS_SSL_VERIFY_PEER_IDENTITY, 2).
```

- `CASS_SSL_VERIFY_NONE` - No verification is performed
- `CASS_SSL_VERIFY_PEER_CERT` - Certificate is present and valid
- `CASS_SSL_VERIFY_PEER_IDENTITY` - IP address matches the certificate's common name or one of its subject alternative names. This implies the certificate is also present.

You can use also a combination like : `?CASS_SSL_VERIFY_PEER_CERT bor ?CASS_SSL_VERIFY_PEER_IDENTITY`

Default: `CASS_SSL_VERIFY_PEER_CERT`

##### protocol_version

Example: {protocol_version, 2}

Sets the protocol version. This will automatically downgrade if to protocol version 1.

Default: 2

##### number_threads_io

Example: {number_threads_io, 1}

Sets the number of IO threads. This is the number of threads that will handle query requests.

Default: 1

##### queue_size_io

Example: {queue_size_io, 4096}

Sets the size of the the fixed size queue that stores pending requests.

Default: 4096

##### queue_size_event

Example: {queue_size_event, 4096}

Sets the size of the the fixed size queue that stores events.

Default: 4096

##### core_connections_host

Example: {core_connections_host, 1}

Sets the number of connections made to each server in each IO thread.

Default: 1

##### max_connections_host

Example: {max_connections_host, 2}

Sets the maximum number of connections made to each server in each IO thread.

Default: 2

##### reconnect_wait_time

Example: {reconnect_wait_time, 2000}

Sets the amount of time to wait before attempting to reconnect.

Default: 2000 milliseconds

##### max_concurrent_creation

Example: {max_concurrent_creation, 1}

Sets the maximum number of connections that will be created concurrently.
Connections are created when the current connections are unable to keep up with request throughput.

Default: 1

##### max_requests_threshold

Example: {max_requests_threshold, 100}

Sets the threshold for the maximum number of concurrent requests in-flight on a connection before creating a new connection.
The number of new connections created will not exceed max_connections_host.

Default: 100

##### requests_per_flush

Example: {requests_per_flush, 128}

Sets the maximum number of requests processed by an IO worker per flush.

Default: 128

##### write_bytes_high_watermark

Example: {write_bytes_high_watermark, 65536}

Sets the high water mark for the number of bytes outstanding on a connection.
Disables writes to a connection if the number of bytes queued exceed this value.

Default: 64 KB

##### write_bytes_low_watermark

Example: {write_bytes_low_watermark, 32768}

Sets the low water mark for number of bytes outstanding on a connection.
After exceeding high water mark bytes, writes will only resume once the number of bytes fall below this value.

Default: 32 KB

##### pending_requests_high_watermark

Example: {pending_requests_high_watermark, 128}

Sets the high water mark for the number of requests queued waiting for a connection in a connection pool.
Disables writes to a host on an IO worker if the number of requests queued exceed this value.

Default: 128 * max_connections_per_host


##### pending_requests_low_watermark

Example: {pending_requests_low_watermark, 64}

Sets the low water mark for the number of requests queued waiting for a connection in a connection pool.
After exceeding high water mark requests, writes to a host will only resume once the number of requests fall below this value.

Default: 64 * max_connections_per_host

##### connect_timeout

Example: {connect_timeout, 5000}

Sets the timeout for connecting to a node.

Default: 5000 milliseconds

##### heartbeat_interval

Example: {heartbeat_interval, 30}

Sets the amount of time between heartbeat messages and controls the amount of time the connection must be idle before sending heartbeat messages.
This is useful for preventing intermediate network devices from dropping connections.

Default: 30 seconds

##### idle_timeout

Example: {idle_timeout, 60}

Sets the amount of time a connection is allowed to be without a successful hearbeat response before being terminated and scheduled for reconnection.

Default: 60 seconds

##### request_timeout

Example: {request_timeout, 12000}

Sets the timeout for waiting for a response from a node.

Default: 12000 milliseconds

##### credentials

Example: {credentials, {<<"username">>, <<"password">>}}

Sets credentials for plain text authentication.

##### load_balance_round_robin

Example: {load_balance_round_robin, true}

Configures the cluster to use round-robin load balancing.
The driver discovers all nodes in a cluster and cycles through them per request. All are considered 'local'.

##### load_balance_dc_aware

Example: {load_balance_dc_aware, {"dc_name", 2, true}}

Configures the cluster to use DC-aware load balancing.
For each query, all live nodes in a primary 'local' DC are tried first, followed by any node from other DCs.

###### Note:

This is the default, and does not need to be called unless switching an existing from another policy or changing settings.
Without further configuration, a default local_dc is chosen from the first connected contact point, and no remote hosts are considered in query plans.
If relying on this mechanism, be sure to use only contact points from the local DC.

###### Params:

*{load_balance_dc_aware, {LocalDc, UsedHostsPerRemoteDc, AllowRemoteDcsForLocalCl}}*

* LocalDc - The primary data center to try first
* UsedHostsPerRemoteDc - The number of host used in each remote DC if no hosts are available in the local dc
* AllowRemoteDcsForLocalCl - Allows remote hosts to be used if no local dc hosts are available and the consistency level is LOCAL_ONE or LOCAL_QUORUM

##### token_aware_routing

Example: {token_aware_routing, true}

Configures the cluster to use token-aware request routing, or not.
This routing policy composes the base routing policy, routing requests first to replicas on nodes considered 'local' by the base load balancing policy.

Default is true (enabled).

#### latency_aware_routing

Example:

- `{latency_aware_routing, true}`
- `{latency_aware_routing, {true, {2.0, 100, 10000, 100 , 50}}}`

Configures the cluster to use latency-aware request routing, or not.
This routing policy is a top-level routing policy.
It uses the base routing policy to determine locality (dc-aware) and/or placement (token-aware) before considering the latency.

###### Params:

{Enabled, {ExclusionThreshold, ScaleMs, RetryPeriodMs, UpdateRateMs, MinMeasured}}

- Enabled : State of the future
- ExclusionThreshold : Controls how much worse the latency must be compared to the average latency of the best performing node before it penalized.
- ScaleMs Controls the weight given to older latencies when calculating the average latency of a node. A bigger scale will give more weight to older latency measurements.
- RetryPeriodMs -  The amount of time a node is penalized by the policy before being given a second chance when the current average latency exceeds the calculated threshold (ExclusionThreshold * BestAverageLatency).
- UpdateRateMs - The rate at  which the best average latency is recomputed.
- MinMeasured - The minimum number of measurements per-host required to be considered by the policy.

Defaults: {false, {2.0, 100, 10000, 100 , 50}}

###### Note: In case you use only true false atom the tuning settings will not change.

##### tcp_nodelay

Example: {tcp_nodelay, false}

Enable/Disable Nagel's algorithm on connections.

Default: true (disabled).

##### tcp_keepalive

Example: {tcp_keepalive, {true, 60}}

Enable/Disable TCP keep-alive

Default: cass_false (disabled).

##### default_consistency_level

Example: {default_consistency_level, ?CASS_CONSISTENCY_ONE}

Set the default consistency level

Default: ?CASS_CONSISTENCY_ONE

### Creating a session

*Currently this is limited to one session per application. This is a Datastax recommendations as well*

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

In case you want to overwrite the default consistency level for that prepare statement use a tuple for the query argument: *{Query, ConsistencyLevelHere}*

Example:

```erlang
ok = erlcass:add_prepare_statement(
                select_blogpost,
                { <<"select * from blogposts where domain = ? LIMIT 1">>, ?CASS_CONSISTENCY_LOCAL_QUORUM }).
```

### Run a prepared statement query

In case the first parameter for *erlcass:execute* is an atom then the driver will try to find the associated prepared statement and to run it.
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

For blocking operations use *erlcass:execute*, for async execution use : *erlcass:async_execute*.
The blocking operation will block the current erlang process (still async into the native code in order to avoid freezing of the VM threads) until will get the result from the cluster.

In case of an async execution the calling process will receive a message of the following form: *{execute_statement_result, Tag, Result}*

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
The datatypes can be found into *erlcass.hrl* file as follow:

```erlang
-define(CASS_TEXT, text).                         %use for (ascii, text, varchar)
-define(CASS_INT, int).                           %use for (int )
-define(CASS_BIGINT, bigint).                     %use for (timestamp, counter, bigint)
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

The third argument is a list of options currently ony `consistency_level` is available. If it's missing the batch will be
executed using the default consistency level value.

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

- erlcass:uuid_gen_time()   -> Generates a V1 (time) UUID
- erlcass:uuid_gen_random() -> Generates a new V4 (random) UUID
- erlcass:uuid_gen_from_ts(Ts) -> Generates a V1 (time) UUID for the specified timestamp
- erlcass:uuid_min_from_ts(Ts) -> Sets the UUID to the minimum V1 (time) value for the specified timestamp,
- erlcass:uuid_max_from_ts(Ts) -> Sets the UUID to the maximum V1 (time) value for the specified timestamp,
- erlcass:uuid_get_ts(Uuid) -> Gets the timestamp for a V1 UUID,
- erlcass:uuid_get_version(Uuid) -> Gets the version for a UUID (V1 or V4)

### Getting metrics

In order to get metrics from the native driver you can use *erlcass:get_metrics().*

##### requests

- min - Minimum in microseconds
- max - Maximum in microseconds
- mean - Mean in microseconds
- stddev - Standard deviation in microseconds
- median - Median in microseconds
- percentile_75th - 75th percentile in microseconds
- percentile_95th - 95th percentile in microseconds
- percentile_98th - 98th percentile in microseconds
- percentile_99th - 99the percentile in microseconds
- percentile_999th - 99.9th percentile in microseconds
- mean_rate - Mean rate in requests per second
- one_minute_rate - 1 minute rate in requests per second
- five_minute_rate - 5 minute rate in requests per second
- fifteen_minute_rate - 15 minute rate in requests per second

##### stats

- total_connections - The total number of connections
- available_connections - The number of connections available to take requests
- exceeded_pending_requests_water_mark - Occurrences when requests exceeded a pool's water mark
- exceeded_write_bytes_water_mark - Occurrences when number of bytes exceeded a connection's water mark

##### errors

- connection_timeouts - Occurrences of a connection timeout
- pending_request_timeouts - Occurrences of requests that timed out waiting for a connection
- request_timeouts - Occurrences of requests that timed out waiting for a request to finish

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

[1]:http://datastax.github.io/cpp-driver/topics/building/
