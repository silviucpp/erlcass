//
//  constants.h
//  erlcass_nif
//
//  Created by silviu on 9/27/16.
//
//

#ifndef ERLCASS_C_SRC_CONSTANTS_H
#define ERLCASS_C_SRC_CONSTANTS_H

namespace erlcass {

//message errors

extern const char kInvalidUuidGeneratorMsg[];
extern const char kClusterObjectFailedToCreateMsg[];
extern const char kClusterObjectNotCreatedMsg[];
extern const char kFailedToAllocResourceMsg[];
extern const char kFailedToGetParentIdMsg[];
extern const char kFailedToCreateCallbackInfoMsg[];
extern const char kFailedToCreateBatchObjectMsg[];
extern const char kBindFailedUnknownColumnType[];
extern const char kFailedToAddUnknownTypeInCollection[];
extern const char kFailedToSetUnknownTypeInTuple[];

//misc atoms

extern const char kAtomOk[];
extern const char kAtomError[];
extern const char kAtomTrue[];
extern const char kAtomFalse[];
extern const char kAtomNull[];
extern const char kAtomBadArg[];
extern const char kAtomConsistencyLevel[];
extern const char kAtomSerialConsistencyLevel[];
extern const char kAtomLogMsgRecord[];

//events atoms

extern const char kAtomSessionConnected[];
extern const char kAtomSessionClosed[];
extern const char kAtomPreparedStatementResult[];
extern const char kAtomExecuteStatementResult[];
extern const char kAtomLogMessageReceived[];

//data types

extern const char kAtomText[];
extern const char kAtomTinyInt[];
extern const char kAtomSmallInt[];
extern const char kAtomInt[];
extern const char kAtomDate[];
extern const char kAtomBigInt[];
extern const char kAtomBlob[];
extern const char kAtomBool[];
extern const char kAtomFloat[];
extern const char kAtomDouble[];
extern const char kAtomInet[];
extern const char kAtomUuid[];
extern const char kAtomDecimal[];
extern const char kAtomList[];
extern const char kAtomSet[];
extern const char kAtomMap[];
extern const char kAtomTuple[];

//cluster settings atoms

extern const char kAtomClusterDefaultConsistencyLevel[];
extern const char kAtomClusterSettingContactPoints[];
extern const char kAtomClusterSettingPort[];
extern const char kAtomClusterSettingProtocolVersion[];
extern const char kAtomClusterSettingNumThreadsIo[];
extern const char kAtomClusterSettingQueueSizeIo[];
extern const char kAtomClusterSettingQueueSizeEvent[];
extern const char kAtomClusterSettingCoreConnectionsPerHost[];
extern const char kAtomClusterSettingMaxConnectionsPerHost[];
extern const char kAtomClusterSettingReconnectWaitTime[];
extern const char kAtomClusterSettingMaxConcurrentCreation[];
extern const char kAtomClusterSettingMaxConcurrentRequestsThreshold[];
extern const char kAtomClusterSettingMaxRequestsPerFlush[];
extern const char kAtomClusterSettingWriteBytesHighWaterMark[];
extern const char kAtomClusterSettingWriteBytesLowWaterMark[];
extern const char kAtomClusterSettingPendingRequestsHighWaterMark[];
extern const char kAtomClusterSettingPendingRequestsLowWaterMark[];
extern const char kAtomClusterSettingConnectTimeout[];
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

}

#endif
