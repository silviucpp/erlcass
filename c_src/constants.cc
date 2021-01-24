#include "constants.h"

namespace erlcass {

// message errors

const char kInvalidUuidGeneratorMsg[] = "invalid uuid generator";
const char kClusterObjectFailedToCreateMsg[] = "failed to create the cluster object";
const char kClusterObjectNotCreatedMsg[] = "cluster object doesn't exist";
const char kFailedToAllocResourceMsg[] = "enif_alloc_resource failed";
const char kFailedToCreateCallbackInfoMsg[] = "failed to create callback info";
const char kFailedToCreateBatchObjectMsg[] = "failed to create batch object";
const char kBindFailedUnknownColumnType[] = "bind failed. not implemented column type";
const char kFailedToSetUnknownType[] = "failed to set unknown type";
const char kUnknownKeyspace[] = "keyspace doesn't exist";
const char kUnknownTable[] = "table doesn't exist";

// misc atoms

const char kAtomOk[] = "ok";
const char kAtomError[] = "error";
const char kAtomTrue[] = "true";
const char kAtomFalse[] = "false";
const char kAtomNull[] = "null";
const char kAtomBadArg[] = "badarg";
const char kAtomOptions[] = "options";
const char kAtomConsistencyLevel[] = "consistency_level";
const char kAtomSerialConsistencyLevel[] = "serial_consistency_level";
const char kAtomLogMsgRecord[] = "log_msg";

// events atoms

const char kAtomSessionConnected[] = "session_connected";
const char kAtomSessionClosed[] = "session_closed";
const char kAtomPreparedStatementResult[] = "prepared_statement_result";
const char kAtomExecuteStatementResult[] = "execute_statement_result";
const char kAtomPagedExecuteStatementResult[] = "paged_execute_statement_result";
const char kAtomPagedExecuteStatementResultHasMore[] = "paged_execute_statement_result_has_more";
const char kAtomLogMessageReceived[] = "log_message_recv";

// data types

const char kAtomAscii[] = "ascii";
const char kAtomBigInt[] = "bigint";
const char kAtomBlob[] = "blob";
const char kAtomBool[] = "boolean";
const char kAtomCounter[] = "counter";
const char kAtomDate[] = "date";
const char kAtomDecimal[] = "decimal";
const char kAtomDouble[] = "double";
const char kAtomFloat[] = "float";
const char kAtomFrozen[] = "frozen";
const char kAtomInet[] = "inet";
const char kAtomInt[] = "int";
const char kAtomList[] = "list";
const char kAtomMap[] = "map";
const char kAtomSet[] = "set";
const char kAtomSmallInt[] = "smallint";
const char kAtomText[] = "text";
const char kAtomTime[] = "time";
const char kAtomTimestamp[] = "timestamp";
const char kAtomTimeUuid[] = "timeuuid";
const char kAtomTinyInt[] = "tinyint";
const char kAtomTuple[] = "tuple";
const char kAtomUuid[] = "uuid";
const char kAtomUdt[] = "udt";
const char kAtomVarchar[] = "varchar";
const char kAtomVarint[] = "varint";

// column types

const char kAtomColumnTypeRegular[] = "regular";
const char kAtomColumnTypePartitionKey[] = "partition_key";
const char kAtomColumnTypeClusteringKey[] = "clustering_key";
const char kAtomColumnTypeStatic[] = "static";
const char kAtomColumnTypeCompactValue[] = "compact_value";

// metadata

const char kAtomMetadataSchemaVersion[] = "version";
const char kAtomColumnMetaColumnName[] = "column_name";
const char kAtomColumnMetaDataType[] = "data_type";
const char kAtomColumnMetaType[] = "type";

// cluster settings atoms

const char kAtomClusterDefaultConsistencyLevel[] = "default_consistency_level";
const char kAtomClusterSettingContactPoints[] = "contact_points";
const char kAtomClusterSettingPort[] = "port";
const char kAtomClusterSettingProtocolVersion[] = "protocol_version";
const char kAtomClusterSettingNumThreadsIo[] = "number_threads_io";
const char kAtomClusterSettingQueueSizeIo[] = "queue_size_io";
const char kAtomClusterSettingCoreConnectionsPerHost[] = "core_connections_host";
const char kAtomClusterSettingConnectTimeout[] = "connect_timeout";
const char kAtomClusterSettingConstantReconnect[] = "constant_reconnect";
const char kAtomClusterSettingExponentialReconnect[] = "exponential_reconnect";
const char kAtomClusterSettingCoalesceDelay[] = "coalesce_delay";
const char kAtomClusterSettingRequestRatio[] = "request_ratio";
const char kAtomClusterSettingMaxSchemaWaitTime[] = "max_schema_wait_time";
const char kAtomClusterSettingTokenAwareRoutingShuffleReplicas[] = "token_aware_routing_shuffle_replicas";
const char kAtomClusterSettingUseHostnameResolution[] = "use_hostname_resolution";
const char kAtomClusterSettingSpeculativeExecutionPolicy[] = "speculative_execution_policy";
const char kAtomClusterSettingMaxReusableWriteObjects[] = "max_reusable_write_objects";
const char kAtomClusterSettingRequestTimeout[] = "request_timeout";
const char kAtomClusterSettingCredentials[] = "credentials";
const char kAtomClusterSettingLoadBalanceRoundRobin[] = "load_balance_round_robin";
const char kAtomClusterSettingLoadBalanceDcAware[] = "load_balance_dc_aware";
const char kAtomClusterSettingTokenAwareRouting[] = "token_aware_routing";
const char kAtomClusterSettingLatencyAwareRouting[] = "latency_aware_routing";
const char kAtomClusterSettingTcpNodelay[] = "tcp_nodelay";
const char kAtomClusterSettingTcpKeepalive[] = "tcp_keepalive";
const char kAtomClusterSettingIdleTimeout[] = "idle_timeout";
const char kAtomClusterSettingHeartbeatInterval[] = "heartbeat_interval";
const char kAtomClusterSettingSsl[] = "ssl";
const char kAtomClusterSettingSslTrustedCerts[] = "trusted_certs";
const char kAtomClusterSettingSslVerifyFlags[] = "verify_flags";
const char kAtomClusterSettingSslCert[] = "cert";
const char kAtomClusterSettingSslPrivateKey[] = "private_key";
const char kAtomClusterSettingRetryPolicy[] = "retry_policy";
const char kAtomClusterSettingRetryPolicyDefault[] = "default";
const char kAtomClusterSettingRetryPolicyFallthrough[] = "fallthrough";

}  // namespace erlcass
