### Changelog:

##### v2.7 (not released yet)

- Improved `erlcass` logging system. In case the logging process it's crashing it's restarted
- Integrated `lager` as dependency
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