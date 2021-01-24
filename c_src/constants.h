#ifndef C_SRC_CONSTANTS_H_
#define C_SRC_CONSTANTS_H_

namespace erlcass {

// message errors

extern const char kInvalidUuidGeneratorMsg[];
extern const char kClusterObjectFailedToCreateMsg[];
extern const char kClusterObjectNotCreatedMsg[];
extern const char kFailedToAllocResourceMsg[];
extern const char kFailedToCreateCallbackInfoMsg[];
extern const char kFailedToCreateBatchObjectMsg[];
extern const char kBindFailedUnknownColumnType[];
extern const char kFailedToSetUnknownType[];
extern const char kUnknownKeyspace[];
extern const char kUnknownTable[];

// misc atoms

extern const char kAtomOk[];
extern const char kAtomError[];
extern const char kAtomTrue[];
extern const char kAtomFalse[];
extern const char kAtomNull[];
extern const char kAtomBadArg[];
extern const char kAtomOptions[];
extern const char kAtomConsistencyLevel[];
extern const char kAtomSerialConsistencyLevel[];
extern const char kAtomLogMsgRecord[];

// events atoms

extern const char kAtomSessionConnected[];
extern const char kAtomSessionClosed[];
extern const char kAtomPreparedStatementResult[];
extern const char kAtomExecuteStatementResult[];
extern const char kAtomPagedExecuteStatementResult[];
extern const char kAtomPagedExecuteStatementResultHasMore[];
extern const char kAtomLogMessageReceived[];

// data types

extern const char kAtomAscii[];
extern const char kAtomBigInt[];
extern const char kAtomBlob[];
extern const char kAtomBool[];
extern const char kAtomCounter[];
extern const char kAtomDate[];
extern const char kAtomDecimal[];
extern const char kAtomDouble[];
extern const char kAtomFloat[];
extern const char kAtomFrozen[];
extern const char kAtomInet[];
extern const char kAtomInt[];
extern const char kAtomList[];
extern const char kAtomMap[];
extern const char kAtomSet[];
extern const char kAtomSmallInt[];
extern const char kAtomText[];
extern const char kAtomTime[];
extern const char kAtomTimestamp[];
extern const char kAtomTimeUuid[];
extern const char kAtomTinyInt[];
extern const char kAtomTuple[];
extern const char kAtomUuid[];
extern const char kAtomUdt[];
extern const char kAtomVarchar[];
extern const char kAtomVarint[];

// column types

extern const char kAtomColumnTypeRegular[];
extern const char kAtomColumnTypePartitionKey[];
extern const char kAtomColumnTypeClusteringKey[];
extern const char kAtomColumnTypeStatic[];
extern const char kAtomColumnTypeCompactValue[];

// metadata

extern const char kAtomMetadataSchemaVersion[];
extern const char kAtomColumnMetaColumnName[];
extern const char kAtomColumnMetaDataType[];
extern const char kAtomColumnMetaType[];

// cluster settings atoms

extern const char kAtomClusterDefaultConsistencyLevel[];
extern const char kAtomClusterSettingContactPoints[];
extern const char kAtomClusterSettingPort[];
extern const char kAtomClusterSettingProtocolVersion[];
extern const char kAtomClusterSettingNumThreadsIo[];
extern const char kAtomClusterSettingQueueSizeIo[];
extern const char kAtomClusterSettingCoreConnectionsPerHost[];
extern const char kAtomClusterSettingConnectTimeout[];
extern const char kAtomClusterSettingConstantReconnect[];
extern const char kAtomClusterSettingExponentialReconnect[];
extern const char kAtomClusterSettingCoalesceDelay[];
extern const char kAtomClusterSettingRequestRatio[];
extern const char kAtomClusterSettingMaxSchemaWaitTime[];
extern const char kAtomClusterSettingTokenAwareRoutingShuffleReplicas[];
extern const char kAtomClusterSettingUseHostnameResolution[];
extern const char kAtomClusterSettingSpeculativeExecutionPolicy[];
extern const char kAtomClusterSettingMaxReusableWriteObjects[];
extern const char kAtomClusterSettingRequestTimeout[];
extern const char kAtomClusterSettingCredentials[];
extern const char kAtomClusterSettingLoadBalanceRoundRobin[];
extern const char kAtomClusterSettingLoadBalanceDcAware[];
extern const char kAtomClusterSettingTokenAwareRouting[];
extern const char kAtomClusterSettingLatencyAwareRouting[];
extern const char kAtomClusterSettingTcpNodelay[];
extern const char kAtomClusterSettingTcpKeepalive[];
extern const char kAtomClusterSettingIdleTimeout[];
extern const char kAtomClusterSettingHeartbeatInterval[];
extern const char kAtomClusterSettingSsl[];
extern const char kAtomClusterSettingSslTrustedCerts[];
extern const char kAtomClusterSettingSslVerifyFlags[];
extern const char kAtomClusterSettingSslCert[];
extern const char kAtomClusterSettingSslPrivateKey[];
extern const char kAtomClusterSettingRetryPolicy[];
extern const char kAtomClusterSettingRetryPolicyDefault[];
extern const char kAtomClusterSettingRetryPolicyFallthrough[];

}  // namespace erlcass

#endif  // C_SRC_CONSTANTS_H_
