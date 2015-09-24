#ifndef erlcass_erlcass_h
#define erlcass_erlcass_h

#include "erl_nif.h"
#include "cassandra.h"

struct atoms
{
    ERL_NIF_TERM atomOk;
    ERL_NIF_TERM atomError;
    ERL_NIF_TERM atomTrue;
    ERL_NIF_TERM atomFalse;
    ERL_NIF_TERM atomNull;
    ERL_NIF_TERM atomConsistencyLevel;
    ERL_NIF_TERM atomLogMsgRecord;
    
    //events atoms
    
    ERL_NIF_TERM atomSessionConnected;
    ERL_NIF_TERM atomSessionClosed;
    ERL_NIF_TERM atomPreparedStatementResult;
    ERL_NIF_TERM atomExecuteStatementResult;
    ERL_NIF_TERM atomLogMessageReceived;
    
    //data types
    
    ERL_NIF_TERM atomList;
    ERL_NIF_TERM atomSet;
    ERL_NIF_TERM atomMap;
    ERL_NIF_TERM atomTuple;
    ERL_NIF_TERM atomText;
    ERL_NIF_TERM atomInt;
    ERL_NIF_TERM atomBigInt;
    ERL_NIF_TERM atomBlob;
    ERL_NIF_TERM atomBool;
    ERL_NIF_TERM atomFloat;
    ERL_NIF_TERM atomDouble;
    ERL_NIF_TERM atomInet;
    ERL_NIF_TERM atomUuid;
    ERL_NIF_TERM atomDecimal;
    
    //cluster setings atoms
    
    ERL_NIF_TERM atomClusterDefaultConsistencyLevel;
    ERL_NIF_TERM atomClusterSettingContactPoints;
    ERL_NIF_TERM atomClusterSettingPort;
    ERL_NIF_TERM atomClusterSettingProtocolVersion;
    ERL_NIF_TERM atomClusterSettingNumThreadsIo;
    ERL_NIF_TERM atomClusterSettingQueueSizeIo;
    ERL_NIF_TERM atomClusterSettingQueueSizeEvent;
    ERL_NIF_TERM atomClusterSettingCoreConnectionsPerHost;
    ERL_NIF_TERM atomClusterSettingMaxConnectionsPerHost;
    ERL_NIF_TERM atomClusterSettingReconnectWaitTime;    
    ERL_NIF_TERM atomClusterSettingMaxConcurrentCreation;
    ERL_NIF_TERM atomClusterSettingMaxConcurrentRequestsThreshold;
    ERL_NIF_TERM atomClusterSettingMaxRequestsPerFlush;
    ERL_NIF_TERM atomClusterSettingWriteBytesHighWaterMark;
    ERL_NIF_TERM atomClusterSettingWriteBytesLowWaterMark;
    ERL_NIF_TERM atomClusterSettingPendingRequestsHighWaterMark;
    ERL_NIF_TERM atomClusterSettingPendingRequestsLowWaterMark;
    ERL_NIF_TERM atomClusterSettingConnectTimeout;
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
};

struct cassandra_data
{
    CassCluster* cluster;
    ErlNifPid log_pid;
    
    CassConsistency defaultConsistencyLevel;
    ErlNifResourceType* resCassSession;
    ErlNifResourceType* resCassPrepared;
    ErlNifResourceType* resCassStatement;
    ErlNifResourceType* resCassUuidGen;
};

extern atoms ATOMS;

#endif
