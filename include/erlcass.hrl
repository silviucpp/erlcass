
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


