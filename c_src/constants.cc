#include "constants.h"

namespace erlcass {

//message errors

const char kInvalidUuidGeneratorMsg[] = "invalid uuid generator";
const char kClusterObjectFailedToCreateMsg[] = "failed to create the cluster object";
const char kClusterObjectNotCreatedMsg[] = "cluster object doesn't exist";
const char kFailedToAllocResourceMsg[] = "enif_alloc_resource failed";
const char kFailedToCreateCallbackInfoMsg[] = "failed to create callback info";
const char kFailedToCreateBatchObjectMsg[] = "failed to create batch object";
const char kBindFailedUnknownColumnType[] = "bind failed. not implemented column type";
const char kFailedToSetUnknownType[] = "failed to set unknown type";

//misc atoms

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

//events atoms

const char kAtomSessionConnected[] = "session_connected";
const char kAtomSessionClosed[] = "session_closed";
const char kAtomPreparedStatementResult[] = "prepared_statement_result";
const char kAtomExecuteStatementResult[] = "execute_statement_result";
const char kAtomLogMessageReceived[] = "log_message_recv";

//data types

const char kAtomText[] = "text";
const char kAtomTinyInt[] = "tinyint";
const char kAtomSmallInt[] = "smallint";
const char kAtomInt[] = "int";
const char kAtomDate[] = "date";
const char kAtomBigInt[] = "bigint";
const char kAtomBlob[] = "blob";
const char kAtomBool[] = "bool";
const char kAtomFloat[] = "float";
const char kAtomDouble[] = "double";
const char kAtomInet[] = "inet";
const char kAtomUuid[] = "uuid";
const char kAtomDecimal[] = "decimal";
const char kAtomList[] = "list";
const char kAtomSet[] = "set";
const char kAtomMap[] = "map";
const char kAtomTuple[] = "tuple";
const char kAtomTimestamp[] = "timestamp";
//cluster settings atoms

const char kAtomClusterDefaultConsistencyLevel[] = "default_consistency_level";
const char kAtomClusterSettingContactPoints[] = "contact_points";
const char kAtomClusterSettingPort[] = "port";
const char kAtomClusterSettingProtocolVersion[] = "protocol_version";
const char kAtomClusterSettingNumThreadsIo[] = "number_threads_io";
const char kAtomClusterSettingQueueSizeIo[] = "queue_size_io";
const char kAtomClusterSettingQueueSizeEvent[] = "queue_size_event";
const char kAtomClusterSettingCoreConnectionsPerHost[] = "core_connections_host";
const char kAtomClusterSettingMaxConnectionsPerHost[] = "max_connections_host";
const char kAtomClusterSettingReconnectWaitTime[] = "reconnect_wait_time";
const char kAtomClusterSettingMaxConcurrentCreation[] = "max_concurrent_creation";
const char kAtomClusterSettingMaxConcurrentRequestsThreshold[] = "max_requests_threshold";
const char kAtomClusterSettingMaxRequestsPerFlush[] = "requests_per_flush";
const char kAtomClusterSettingWriteBytesHighWaterMark[] = "write_bytes_high_watermark";
const char kAtomClusterSettingWriteBytesLowWaterMark[] = "write_bytes_low_watermark";
const char kAtomClusterSettingPendingRequestsHighWaterMark[] = "pending_requests_high_watermark";
const char kAtomClusterSettingPendingRequestsLowWaterMark[] = "pending_requests_low_watermark";
const char kAtomClusterSettingConnectTimeout[] = "connect_timeout";
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

}
