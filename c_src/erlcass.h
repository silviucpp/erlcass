#ifndef C_SRC_ERLCASS_H_
#define C_SRC_ERLCASS_H_

#include "erl_nif.h"
#include "cassandra.h"

struct atoms
{
    ERL_NIF_TERM atomOk;
    ERL_NIF_TERM atomError;
    ERL_NIF_TERM atomTrue;
    ERL_NIF_TERM atomFalse;
    ERL_NIF_TERM atomNull;
    ERL_NIF_TERM atomBadArg;
    ERL_NIF_TERM atomOptions;
    ERL_NIF_TERM atomConsistencyLevel;
    ERL_NIF_TERM atomSerialConsistencyLevel;
    ERL_NIF_TERM atomLogMsgRecord;

    // events atoms

    ERL_NIF_TERM atomSessionConnected;
    ERL_NIF_TERM atomSessionClosed;
    ERL_NIF_TERM atomPreparedStatementResult;
    ERL_NIF_TERM atomExecuteStatementResult;
    ERL_NIF_TERM atomPagedExecuteStatementResult;
    ERL_NIF_TERM atomPagedExecuteStatementResultHasMore;
    ERL_NIF_TERM atomLogMessageReceived;

    // data types

    ERL_NIF_TERM atomAscii;
    ERL_NIF_TERM atomBigInt;
    ERL_NIF_TERM atomBlob;
    ERL_NIF_TERM atomBool;
    ERL_NIF_TERM atomCounter;
    ERL_NIF_TERM atomDate;
    ERL_NIF_TERM atomDecimal;
    ERL_NIF_TERM atomDouble;
    ERL_NIF_TERM atomFloat;
    ERL_NIF_TERM atomFrozen;
    ERL_NIF_TERM atomInet;
    ERL_NIF_TERM atomInt;
    ERL_NIF_TERM atomList;
    ERL_NIF_TERM atomMap;
    ERL_NIF_TERM atomSet;
    ERL_NIF_TERM atomSmallInt;
    ERL_NIF_TERM atomText;
    ERL_NIF_TERM atomTime;
    ERL_NIF_TERM atomTimestamp;
    ERL_NIF_TERM atomTimeUuid;
    ERL_NIF_TERM atomTinyInt;
    ERL_NIF_TERM atomTuple;
    ERL_NIF_TERM atomUuid;
    ERL_NIF_TERM atomUdt;
    ERL_NIF_TERM atomVarchar;
    ERL_NIF_TERM atomVarint;

    // column types

    ERL_NIF_TERM atomColumnTypeRegular;
    ERL_NIF_TERM atomColumnTypePartitionKey;
    ERL_NIF_TERM atomColumnTypeClusteringKey;
    ERL_NIF_TERM atomColumnTypeStatic;
    ERL_NIF_TERM atomColumnTypeCompactValue;

    // metadata

    ERL_NIF_TERM atomMetadataSchemaVersion;
    ERL_NIF_TERM atomColumnMetaColumnName;
    ERL_NIF_TERM atomColumnMetaDataType;
    ERL_NIF_TERM atomColumnMetaType;

    // cluster setings atoms

    ERL_NIF_TERM atomClusterDefaultConsistencyLevel;
    ERL_NIF_TERM atomClusterSettingContactPoints;
    ERL_NIF_TERM atomClusterSettingPort;
    ERL_NIF_TERM atomClusterSettingProtocolVersion;
    ERL_NIF_TERM atomClusterSettingNumThreadsIo;
    ERL_NIF_TERM atomClusterSettingQueueSizeIo;
    ERL_NIF_TERM atomClusterSettingCoreConnectionsPerHost;
    ERL_NIF_TERM atomClusterSettingConnectTimeout;
    ERL_NIF_TERM atomClusterSettingConstantReconnect;
    ERL_NIF_TERM atomClusterSettingExponentialReconnect;
    ERL_NIF_TERM atomClusterSettingCoalesceDelay;
    ERL_NIF_TERM atomClusterSettingRequestRatio;
    ERL_NIF_TERM atomClusterSettingMaxSchemaWaitTime;
    ERL_NIF_TERM atomClusterSettingTokenAwareRoutingShuffleReplicas;
    ERL_NIF_TERM atomClusterSettingUseHostnameResolution;
    ERL_NIF_TERM atomClusterSettingSpeculativeExecutionPolicy;
    ERL_NIF_TERM atomClusterSettingMaxReusableWriteObjects;
    ERL_NIF_TERM atomClusterSettingRequestTimeout;
    ERL_NIF_TERM atomClusterSettingCredentials;
    ERL_NIF_TERM atomClusterSettingLoadBalanceRoundRobin;
    ERL_NIF_TERM atomClusterSettingLoadBalanceDcAware;
    ERL_NIF_TERM atomClusterSettingTokenAwareRouting;
    ERL_NIF_TERM atomClusterSetringLatencyAwareRouting;
    ERL_NIF_TERM atomClusterSettingTcpNodelay;
    ERL_NIF_TERM atomClusterSettingTcpKeepalive;
    ERL_NIF_TERM atomClusterSettingHeartbeatInterval;
    ERL_NIF_TERM atomClusterSettingIdleTimeout;
    ERL_NIF_TERM atomClusterSettingSsl;
    ERL_NIF_TERM atomClusterSettingSslTrustedCerts;
    ERL_NIF_TERM atomClusterSettingSslVerifyFlags;
    ERL_NIF_TERM atomClusterSettingSslCert;
    ERL_NIF_TERM atomClusterSettingSslPrivateKey;
    ERL_NIF_TERM atomClusterSettingRetryPolicy;
    ERL_NIF_TERM atomClusterSettingRetryPolicyDefault;
    ERL_NIF_TERM atomClusterSettingRetryPolicyFallthrough;
};

struct cassandra_data
{
    CassCluster* cluster;
    CassUuidGen* uuid_gen;
    ErlNifPid log_pid;

    CassConsistency defaultConsistencyLevel;
    ErlNifResourceType* resCassSession;
    ErlNifResourceType* resCassPrepared;
    ErlNifResourceType* resCassStatement;
};

extern atoms ATOMS;

#endif  // C_SRC_ERLCASS_H_
