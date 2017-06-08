#include "erlcass.h"
#include "nif_cass_cluster.h"
#include "nif_cass_session.h"
#include "nif_cass_prepared.h"
#include "nif_cass_statement.h"
#include "nif_cass_uuid.h"
#include "nif_date_time.h"
#include "nif_utils.h"
#include "constants.h"

atoms ATOMS;

void open_resources(ErlNifEnv* env, cassandra_data* data)
{
    ErlNifResourceFlags flags =  static_cast<ErlNifResourceFlags>(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
    data->resCassSession = enif_open_resource_type(env, NULL, "enif_cass_session", nif_cass_session_free, flags, NULL);
    data->resCassPrepared = enif_open_resource_type(env, NULL, "enif_cass_prepared", nif_cass_prepared_free, flags, NULL);
    data->resCassStatement = enif_open_resource_type(env, NULL, "enif_cass_statement", nif_cass_statement_free, flags, NULL);
}

int on_nif_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    ATOMS.atomOk = make_atom(env, erlcass::kAtomOk);
    ATOMS.atomError = make_atom(env, erlcass::kAtomError);
    ATOMS.atomTrue = make_atom(env, erlcass::kAtomTrue);
    ATOMS.atomFalse = make_atom(env, erlcass::kAtomFalse);
    ATOMS.atomNull = make_atom(env, erlcass::kAtomNull);
    ATOMS.atomBadArg = make_atom(env, erlcass::kAtomBadArg);
    ATOMS.atomOptions = make_atom(env, erlcass::kAtomOptions);
    ATOMS.atomConsistencyLevel = make_atom(env, erlcass::kAtomConsistencyLevel);
    ATOMS.atomSerialConsistencyLevel = make_atom(env, erlcass::kAtomSerialConsistencyLevel);
    ATOMS.atomLogMsgRecord = make_atom(env, erlcass::kAtomLogMsgRecord);
    
    //events atoms
    
    ATOMS.atomSessionConnected = make_atom(env, erlcass::kAtomSessionConnected);
    ATOMS.atomSessionClosed = make_atom(env, erlcass::kAtomSessionClosed);
    ATOMS.atomPreparedStatementResult = make_atom(env, erlcass::kAtomPreparedStatementResult);
    ATOMS.atomExecuteStatementResult = make_atom(env, erlcass::kAtomExecuteStatementResult);
    ATOMS.atomLogMessageReceived = make_atom(env, erlcass::kAtomLogMessageReceived);
        
    //cluster settings atoms
    
    ATOMS.atomClusterDefaultConsistencyLevel = make_atom(env, erlcass::kAtomClusterDefaultConsistencyLevel);
    ATOMS.atomClusterSettingContactPoints = make_atom(env, erlcass::kAtomClusterSettingContactPoints);
    ATOMS.atomClusterSettingPort = make_atom(env, erlcass::kAtomClusterSettingPort);
    ATOMS.atomClusterSettingProtocolVersion = make_atom(env, erlcass::kAtomClusterSettingProtocolVersion);
    ATOMS.atomClusterSettingNumThreadsIo = make_atom(env, erlcass::kAtomClusterSettingNumThreadsIo);
    ATOMS.atomClusterSettingQueueSizeIo = make_atom(env, erlcass::kAtomClusterSettingQueueSizeIo);
    ATOMS.atomClusterSettingQueueSizeEvent = make_atom(env, erlcass::kAtomClusterSettingQueueSizeEvent);
    ATOMS.atomClusterSettingCoreConnectionsPerHost = make_atom(env, erlcass::kAtomClusterSettingCoreConnectionsPerHost);
    ATOMS.atomClusterSettingMaxConnectionsPerHost = make_atom(env, erlcass::kAtomClusterSettingMaxConnectionsPerHost);
    ATOMS.atomClusterSettingReconnectWaitTime = make_atom(env, erlcass::kAtomClusterSettingReconnectWaitTime);
    ATOMS.atomClusterSettingMaxConcurrentCreation = make_atom(env, erlcass::kAtomClusterSettingMaxConcurrentCreation);
    ATOMS.atomClusterSettingMaxConcurrentRequestsThreshold = make_atom(env, erlcass::kAtomClusterSettingMaxConcurrentRequestsThreshold);
    ATOMS.atomClusterSettingMaxRequestsPerFlush = make_atom(env, erlcass::kAtomClusterSettingMaxRequestsPerFlush);
    ATOMS.atomClusterSettingWriteBytesHighWaterMark = make_atom(env, erlcass::kAtomClusterSettingWriteBytesHighWaterMark);
    ATOMS.atomClusterSettingWriteBytesLowWaterMark = make_atom(env, erlcass::kAtomClusterSettingWriteBytesLowWaterMark);
    ATOMS.atomClusterSettingPendingRequestsHighWaterMark = make_atom(env, erlcass::kAtomClusterSettingPendingRequestsHighWaterMark);
    ATOMS.atomClusterSettingPendingRequestsLowWaterMark = make_atom(env, erlcass::kAtomClusterSettingPendingRequestsLowWaterMark);
    ATOMS.atomClusterSettingConnectTimeout = make_atom(env, erlcass::kAtomClusterSettingConnectTimeout);
    ATOMS.atomClusterSettingRequestTimeout = make_atom(env, erlcass::kAtomClusterSettingRequestTimeout);
    ATOMS.atomClusterSettingCredentials = make_atom(env, erlcass::kAtomClusterSettingCredentials);
    ATOMS.atomClusterSettingLoadBalanceRoundRobin = make_atom(env, erlcass::kAtomClusterSettingLoadBalanceRoundRobin);
    ATOMS.atomClusterSettingLoadBalanceDcAware = make_atom(env, erlcass::kAtomClusterSettingLoadBalanceDcAware);
    ATOMS.atomClusterSettingTokenAwareRouting = make_atom(env, erlcass::kAtomClusterSettingTokenAwareRouting);
    ATOMS.atomClusterSetringLatencyAwareRouting = make_atom(env, erlcass::kAtomClusterSettingLatencyAwareRouting);
    ATOMS.atomClusterSettingTcpNodelay = make_atom(env, erlcass::kAtomClusterSettingTcpNodelay);
    ATOMS.atomClusterSettingTcpKeepalive = make_atom(env, erlcass::kAtomClusterSettingTcpKeepalive);
    ATOMS.atomClusterSettingHeartbeatInterval = make_atom(env, erlcass::kAtomClusterSettingHeartbeatInterval);
    ATOMS.atomClusterSettingIdleTimeout = make_atom(env, erlcass::kAtomClusterSettingIdleTimeout);
    ATOMS.atomClusterSettingSsl = make_atom(env, erlcass::kAtomClusterSettingSsl);
    ATOMS.atomClusterSettingSslTrustedCerts = make_atom(env, erlcass::kAtomClusterSettingSslTrustedCerts);
    ATOMS.atomClusterSettingSslVerifyFlags = make_atom(env, erlcass::kAtomClusterSettingSslVerifyFlags);
    ATOMS.atomClusterSettingSslCert = make_atom(env, erlcass::kAtomClusterSettingSslCert);
    ATOMS.atomClusterSettingSslPrivateKey = make_atom(env, erlcass::kAtomClusterSettingSslPrivateKey);
    
    cassandra_data* data = static_cast<cassandra_data*>(enif_alloc(sizeof(cassandra_data)));
    data->cluster = NULL;
    data->uuid_gen = cass_uuid_gen_new();
    data->defaultConsistencyLevel = CASS_CONSISTENCY_LOCAL_QUORUM;
    
    open_resources(env, data);
    
    *priv_data = data;
    return 0;
}

void on_nif_unload(ErlNifEnv* env, void* priv_data)
{
    cassandra_data* data = static_cast<cassandra_data*>(priv_data);
    
    if(data->cluster)
        cass_cluster_free(data->cluster);
    
    if(data->uuid_gen)
        cass_uuid_gen_free(data->uuid_gen);

    enif_free(data);
}

int on_nif_upgrade(ErlNifEnv* env, void** priv, void** old_priv, ERL_NIF_TERM info)
{
    cassandra_data* old_data = static_cast<cassandra_data*>(*old_priv);
    
    cassandra_data* data = static_cast<cassandra_data*>(enif_alloc(sizeof(cassandra_data)));
    data->cluster = old_data->cluster;
    data->uuid_gen = old_data->uuid_gen;
    data->defaultConsistencyLevel = old_data->defaultConsistencyLevel;
    open_resources(env, data);
    
    old_data->cluster = NULL;
    old_data->uuid_gen = NULL;
    *priv = data;
    
    return 0;
}

static ErlNifFunc nif_funcs[] =
{
    //CassCluster
    
    {"cass_cluster_create", 0, nif_cass_cluster_create},
    {"cass_cluster_release", 0, nif_cass_cluster_release},
    {"cass_log_set_callback", 1, nif_cass_log_set_callback},
    {"cass_log_set_level", 1, nif_cass_log_set_level},
    {"cass_cluster_set_options", 1, nif_cass_cluster_set_options},
    
    //CassSession
    
    {"cass_session_new", 0, nif_cass_session_new},
    {"cass_session_connect", 2, nif_cass_session_connect},
    {"cass_session_connect", 3, nif_cass_session_connect},
    {"cass_session_close", 2, nif_cass_session_close},
    {"cass_session_prepare", 4, nif_cass_session_prepare},
    
    {"cass_prepared_bind", 1, nif_cass_prepared_bind},
    {"cass_statement_new", 1, nif_cass_statement_new},
    {"cass_statement_bind_parameters", 3, nif_cass_statement_bind_parameters},
    {"cass_session_execute", 4, nif_cass_session_execute},
    {"cass_session_execute_batch", 5, nif_cass_session_execute_batch},
    {"cass_session_get_metrics", 1, nif_cass_session_get_metrics},
    
    //CassUuidGen
    
    {"cass_uuid_gen_time", 0, nif_cass_uuid_gen_time},
    {"cass_uuid_gen_random", 0, nif_cass_uuid_gen_random},
    {"cass_uuid_gen_from_time", 1, nif_cass_uuid_gen_from_time},
    
    //CassUuid
    
    {"cass_uuid_min_from_time", 1, nif_cass_uuid_min_from_time},
    {"cass_uuid_max_from_time", 1, nif_cass_uuid_max_from_time},
    {"cass_uuid_timestamp", 1, nif_cass_uuid_timestamp},
    {"cass_uuid_version", 1, nif_cass_uuid_version},
    
    //Date Time functions
    
    {"cass_date_from_epoch", 1, nif_cass_date_from_epoch},
    {"cass_time_from_epoch", 1, nif_cass_time_from_epoch},
    {"cass_date_time_to_epoch", 2, nif_cass_date_time_to_epoch}
    
};

ERL_NIF_INIT(erlcass_nif, nif_funcs, on_nif_load, NULL, on_nif_upgrade, on_nif_unload)

