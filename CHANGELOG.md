### Changelog:

##### v4.0.0

Note: This is a major update due to upgrade of the cpp-driver to a major version which use a different internal 
architecture. 

- Update cpp-driver to 2.13.0
- Add support for speculative execution (see `speculative_execution_policy`)
- Removed deprecated cluster configs: 
    - `queue_size_event`
    - `max_connections_host`
    - `reconnect_wait_time`
    - `max_concurrent_creation`
    - `max_requests_threshold`
    - `requests_per_flush`
Add support for new cluster configs (for more details see wiki):
    - `constant_reconnect`
    - `exponential_reconnect`
    - `coalesce_delay`
    - `request_ratio`
    - `max_schema_wait_time`
    - `token_aware_routing_shuffle_replicas`
    - `use_hostname_resolution`
    - `speculative_execution_policy`
    - `max_reusable_write_objects`

- Removed deprecated retry policy: `downgrading_consistency`

##### v3.2.6

- Fix hex.pm package
- Cleaned the code using cppcheck and cpplint

##### v3.2.5

- Use epoch day offset when returning date types
- Fix gcc 8 build
- Fix crash in UDT types (https://github.com/silviucpp/erlcass/issues/42)

##### v3.2.4

- Fix a critical memory issue discovered by running the VM in debug mode.
- Update lager

##### v3.2.3

- Add Trevis CI (Thanks to Gonçalo Tomás)
- Add the proper fix for libuv-dev on Ubuntu > 14.04
- Update lager

##### v3.2.2

- Fix the way libuv-dev is installed on Ubuntu > 14.04 (Thanks to Gonçalo Tomás)

##### v3.2.1

- Fix for hex package

##### v3.2.0

- Prepare for submitting to hex.pm (Thanks to Gonçalo Tomás)
- Fix log pid bug (Thanks to Cibin George)
- Update cpp-driver to 2.9.0

##### v3.1

- Update cpp-driver to 2.8.1
- `write_bytes_high_watermark` and `write_bytes_low_watermark` options were removed
- `pending_requests_high_watermark` and `pending_requests_low_watermark` options were removed
- the following stats are removed: `available_connections`, `exceeded_pending_requests_water_mark`, `exceeded_write_bytes_water_mark`

##### v3.0

- Update cpp-driver to 2.7.1
- Add support for Schema metadata api
- Return column names and types along with results (breaks compatibility with previous versions)
- Add support for retry policy settings

Compatibility changes: This versions breaks the API. All query results will return in case of success:
- `ok` instead `{ok, []}` for all DDL and DML queries (because they never returns any column or row)
- `{ok, Columns, Rows}` instead `{ok, Rows}`, where also each row is encoded as a list not as a tuple as was before.

##### v2.9

- Updated cpp-driver to 2.7.0
- Updated lager to 3.4.2
- Fix for segmentation fault when we try to bind more arguments than we specified (#14).
- API breaking changes:
    - non prepared statements are executed now using `query/1`, `query_async/1` and `query_new_statement/1` (used when should run inside a batch)
    - the following methods are removed: `async_execute_statement/1`, `async_execute_statement/3`, `execute_statement/1`
    - removed support for binding non prepared statements.
- Add support for user-defined type (UDT): decode as proplist.
- Major code refactoring in the way the nif terms are bind in native cass statements

##### v2.8

- Updated cpp-driver to 2.5.0
- Updated lager to 3.2.4
- Fix several bugs in compiling scripts
- Don't throw exception in case of bad arguments. Return {error, badarg} instead
- Add more descriptive errors in case of bad options
- Add versions for `async_execute` where caller can specify the process that's going to receive the response and the tag used to match
the request with the response.
- Add support for fire and forget async requests (response is never sent back, it's only logged in case fails). Use `ReceiverPid` = null in
`async_execute/5` or `async_execute_statement/3`.

##### v2.7

- Integrated `lager` as dependency
- Improved logging system. In case the logging process it's crashing it's restarted
- Removed support for the following methods: `set_cluster_options/1`, `create_session/1,` and `set_log_function/1`
- Proper restarting `erlcass` process in case dies and also reprepare all statements again

##### v2.6

- Improved performances for `gen_time/0`, `gen_random/0` and `gen_from_ts/1` from `erlcass_uuid`
- Small internal refactoring for constants

##### v2.5

- Updated cpp-driver to 2.4.3 (require c++ 11)
- Compatible with both `rebar` and `rebar3`

##### v2.4

- Updated cpp-driver to 2.4.2
- Improved the native code build speed
- Add support for setting serial consistency level

##### v2.3

- Removed the necessity of `gen_server` calls for prepared statements. Observed this as being a bottleneck under heavy load.
- Removed from erlcass module all methods starting with `uuid_*` and `date_from_epoch/1`, `time_from_epoch/1`, `date_time_to_epoch/2`.
  Instead this functions you can use the one from `erlcass_uuid` and `erlcass_time` modules.
- Updated cpp-driver to 2.3.0
- Internal code refactoring

##### v2.2

- Changed the default consistency from `CASS_CONSISTENCY_ONE` to `CASS_CONSISTENCY_LOCAL_QUORUM`
- Updated the cpp-driver to 2.2.0

##### v2.1

- Add support for Cassandra 2.2 data types `tinyint` and `smallint`
- Add support for the Cassandra 2.2 `date` and `time` data types
- Add support for functions to convert from Unix Epoch time (in seconds) to and from the Cassandra `date` and `time` types
- Small improvements
- Refactoring the build dependencies script

##### v2.0

- Added support for logs from native driver
- Added support for tuples
- Added support for nested collections
- Based on cpp-driver 2.2.0-beta1
- Interfaces changes: `bind_prepared_params` replaced by `bind_prepared_params_by_name` and `bind_prepared_params_by_index`
- Add support for `async_execute/1` and `execute/1` (should be used when no binding params available)
- Add support for `async_execute/3` and `execute/3` (second parameter should be used to specify the binding type - by name or index)
- By default `async_execute/2` and `execute/2` are binding the params by index

##### v1.0

- Initial implementation supporting most of the features available in Datastax cpp-driver 1.0.3
