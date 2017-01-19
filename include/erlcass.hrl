-author("silviu.caragea").

-define(CASS_NULL, null).

%consistency level

-define(CASS_CONSISTENCY_UNKNOWN, 65535).
-define(CASS_CONSISTENCY_ANY, 0).
-define(CASS_CONSISTENCY_ONE, 1).
-define(CASS_CONSISTENCY_TWO, 2).
-define(CASS_CONSISTENCY_THREE, 3).
-define(CASS_CONSISTENCY_QUORUM, 4).
-define(CASS_CONSISTENCY_ALL, 5).
-define(CASS_CONSISTENCY_LOCAL_QUORUM, 6).
-define(CASS_CONSISTENCY_EACH_QUORUM, 7).
-define(CASS_CONSISTENCY_SERIAL, 8).
-define(CASS_CONSISTENCY_LOCAL_SERIAL, 9).
-define(CASS_CONSISTENCY_LOCAL_ONE, 10).

%batch types

-define(CASS_BATCH_TYPE_LOGGED, 0).
-define(CASS_BATCH_TYPE_UNLOGGED, 1).
-define(CASS_BATCH_TYPE_COUNTER, 2).

%ssl verification performed on the peer's certificate

-define(CASS_SSL_VERIFY_NONE, 0).
-define(CASS_SSL_VERIFY_PEER_CERT, 1).
-define(CASS_SSL_VERIFY_PEER_IDENTITY, 2).

%metadata types for index binding on non prepared statements

-define(CASS_TEXT, text).                                           %use for (ascii, text, varchar)
-define(CASS_TINYINT, tinyint).                                     %use for (tinyint)
-define(CASS_SMALLINT, smallint).                                   %use for (smallint)
-define(CASS_INT, int).                                             %use for (int)
-define(CASS_DATE, date).                                           %use for (date)
-define(CASS_BIGINT, bigint).                                       %use for (timestamp, counter, bigint, time)
-define(CASS_BLOB, blob).                                           %use for (varint, blob)
-define(CASS_BOOLEAN, bool).                                        %use for (bool)
-define(CASS_FLOAT, float).                                         %use for (float)
-define(CASS_DOUBLE, double).                                       %use for (double)
-define(CASS_INET, inet).                                           %use for (inet)
-define(CASS_UUID, uuid).                                           %use for (timeuuid, uuid)
-define(CASS_DECIMAL, decimal).                                     %use for (decimal)
-define(CASS_LIST(ValueType), {list, ValueType}).                   %use for list
-define(CASS_SET(ValueType), {set, ValueType}).                     %use for set
-define(CASS_MAP(KeyType, ValueType), {map, KeyType, ValueType}).   %use for map
-define(CASS_TUPLE(Types), {tuple, Types}).                         %use for tuples

%binding type

-define(BIND_BY_INDEX, 1).
-define(BIND_BY_NAME, 2).

%log level

-define(CASS_LOG_DISABLED, 0).
-define(CASS_LOG_CRITICAL, 1).
-define(CASS_LOG_ERROR, 2).
-define(CASS_LOG_WARN, 3).
-define(CASS_LOG_INFO, 4).
-define(CASS_LOG_DEBUG,5).
-define(CASS_LOG_TRACE, 6).

-record(erlcass_stm, {session, stm}).

-type reason()          :: term().
-type error()           :: {error, reason()}.
-type query()           :: binary() | {binary(), integer()} | {binary(), list()}.
-type statement_ref()   :: reference() | #erlcass_stm{}.
-type bind_type()       :: ?BIND_BY_INDEX | ?BIND_BY_NAME.
-type batch_type()      :: ?CASS_BATCH_TYPE_LOGGED | ?CASS_BATCH_TYPE_UNLOGGED | ?CASS_BATCH_TYPE_COUNTER.
-type tag()             :: reference().
