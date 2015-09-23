#include "erlcass.h"
#include "nif_cass_cluster.h"
#include "nif_cass_session.h"
#include "nif_cass_prepared.h"
#include "nif_cass_statement.h"
#include "nif_cass_uuid.h"
#include "nif_utils.h"

const char kAtomOk[] = "ok";
const char kAtomError[] = "error";
const char kAtomTrue[] = "true";
const char kAtomFalse[] = "false";
const char kAtomNull[] = "null";
const char kAtomConsistencyLevel[] = "consistency_level";

//events atoms

const char kAtomSessionConnected[] = "session_connected";
const char kAtomSessionClosed[] = "session_closed";
const char kAtomPreparedStatementResult[] = "prepared_statememt_result";
const char kAtomExecuteStatementResult[] = "execute_statement_result";

//data types

const char kAtomText[] = "text";
const char kAtomInt[] = "int";
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
const char kAtomClusterSettingSsl[] = "ssl";
const char kAtomClusterSettingSslTrustedCerts[] = "trusted_certs";
const char kAtomClusterSettingSslVerifyFlags[] = "verify_flags";
const char kAtomClusterSettingSslCert[] = "cert";
const char kAtomClusterSettingSslPrivateKey[] = "private_key";

atoms ATOMS;

void open_resources(ErlNifEnv* env, cassandra_data* data)
{
    ErlNifResourceFlags flags =  static_cast<ErlNifResourceFlags>(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);
    data->resCassSession = enif_open_resource_type(env, NULL, "enif_cass_session", nif_cass_session_free, flags, NULL);
    data->resCassPrepared = enif_open_resource_type(env, NULL, "enif_cass_prepared", nif_cass_prepared_free, flags, NULL);
    data->resCassStatement = enif_open_resource_type(env, NULL, "enif_cass_statement", nif_cass_statement_free, flags, NULL);
    data->resCassUuidGen = enif_open_resource_type(env, NULL, "enif_cass_uuid_gen", nif_cass_uuid_gen_free, flags, NULL);
}

int on_nif_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    ATOMS.atomOk = make_atom(env, kAtomOk);
    ATOMS.atomError = make_atom(env, kAtomError);
    ATOMS.atomTrue = make_atom(env, kAtomTrue);
    ATOMS.atomFalse = make_atom(env, kAtomFalse);
    ATOMS.atomNull = make_atom(env, kAtomNull);
    ATOMS.atomConsistencyLevel = make_atom(env, kAtomConsistencyLevel);
    
    //events atoms
    
    ATOMS.atomSessionConnected = make_atom(env, kAtomSessionConnected);
    ATOMS.atomSessionClosed = make_atom(env, kAtomSessionClosed);
    ATOMS.atomPreparedStatementResult = make_atom(env, kAtomPreparedStatementResult);
    ATOMS.atomExecuteStatementResult = make_atom(env, kAtomExecuteStatementResult);
    
    //data types
    
    ATOMS.atomText = make_atom(env, kAtomText);
    ATOMS.atomInt = make_atom(env, kAtomInt);
    ATOMS.atomBigInt = make_atom(env, kAtomBigInt);
    ATOMS.atomBlob = make_atom(env, kAtomBlob);
    ATOMS.atomBool = make_atom(env, kAtomBool);
    ATOMS.atomFloat = make_atom(env, kAtomFloat);
    ATOMS.atomDouble = make_atom(env, kAtomDouble);
    ATOMS.atomInet = make_atom(env, kAtomInet);
    ATOMS.atomUuid = make_atom(env, kAtomUuid);
    ATOMS.atomDecimal = make_atom(env, kAtomDecimal);
    ATOMS.atomList = make_atom(env, kAtomList);
    ATOMS.atomSet = make_atom(env, kAtomSet);
    ATOMS.atomMap = make_atom(env, kAtomMap);
    ATOMS.atomTuple = make_atom(env, kAtomTuple);
    
    //cluster settings atoms
    
    ATOMS.atomClusterDefaultConsistencyLevel = make_atom(env, kAtomClusterDefaultConsistencyLevel);
    ATOMS.atomClusterSettingContactPoints = make_atom(env, kAtomClusterSettingContactPoints);
    ATOMS.atomClusterSettingPort = make_atom(env, kAtomClusterSettingPort);
    ATOMS.atomClusterSettingProtocolVersion = make_atom(env, kAtomClusterSettingProtocolVersion);
    ATOMS.atomClusterSettingNumThreadsIo = make_atom(env, kAtomClusterSettingNumThreadsIo);
    ATOMS.atomClusterSettingQueueSizeIo = make_atom(env, kAtomClusterSettingQueueSizeIo);
    ATOMS.atomClusterSettingQueueSizeEvent = make_atom(env, kAtomClusterSettingQueueSizeEvent);
    ATOMS.atomClusterSettingCoreConnectionsPerHost = make_atom(env, kAtomClusterSettingCoreConnectionsPerHost);
    ATOMS.atomClusterSettingMaxConnectionsPerHost = make_atom(env, kAtomClusterSettingMaxConnectionsPerHost);
    ATOMS.atomClusterSettingReconnectWaitTime = make_atom(env, kAtomClusterSettingReconnectWaitTime);
    ATOMS.atomClusterSettingMaxConcurrentCreation = make_atom(env, kAtomClusterSettingMaxConcurrentCreation);
    ATOMS.atomClusterSettingMaxConcurrentRequestsThreshold = make_atom(env, kAtomClusterSettingMaxConcurrentRequestsThreshold);
    ATOMS.atomClusterSettingMaxRequestsPerFlush = make_atom(env, kAtomClusterSettingMaxRequestsPerFlush);
    ATOMS.atomClusterSettingWriteBytesHighWaterMark = make_atom(env, kAtomClusterSettingWriteBytesHighWaterMark);
    ATOMS.atomClusterSettingWriteBytesLowWaterMark = make_atom(env, kAtomClusterSettingWriteBytesLowWaterMark);
    ATOMS.atomClusterSettingPendingRequestsHighWaterMark = make_atom(env, kAtomClusterSettingPendingRequestsHighWaterMark);
    ATOMS.atomClusterSettingPendingRequestsLowWaterMark = make_atom(env, kAtomClusterSettingPendingRequestsLowWaterMark);
    ATOMS.atomClusterSettingConnectTimeout = make_atom(env, kAtomClusterSettingConnectTimeout);
    ATOMS.atomClusterSettingRequestTimeout = make_atom(env, kAtomClusterSettingRequestTimeout);
    ATOMS.atomClusterSettingCredentials = make_atom(env, kAtomClusterSettingCredentials);
    ATOMS.atomClusterSettingLoadBalanceRoundRobin = make_atom(env, kAtomClusterSettingLoadBalanceRoundRobin);
    ATOMS.atomClusterSettingLoadBalanceDcAware = make_atom(env, kAtomClusterSettingLoadBalanceDcAware);
    ATOMS.atomClusterSettingTokenAwareRouting = make_atom(env, kAtomClusterSettingTokenAwareRouting);
    ATOMS.atomClusterSetringLatencyAwareRouting = make_atom(env, kAtomClusterSettingLatencyAwareRouting);
    ATOMS.atomClusterSettingTcpNodelay = make_atom(env, kAtomClusterSettingTcpNodelay);
    ATOMS.atomClusterSettingTcpKeepalive = make_atom(env, kAtomClusterSettingTcpKeepalive);
    ATOMS.atomClusterSettingSsl = make_atom(env, kAtomClusterSettingSsl);
    ATOMS.atomClusterSettingSslTrustedCerts = make_atom(env, kAtomClusterSettingSslTrustedCerts);
    ATOMS.atomClusterSettingSslVerifyFlags = make_atom(env, kAtomClusterSettingSslVerifyFlags);
    ATOMS.atomClusterSettingSslCert = make_atom(env, kAtomClusterSettingSslCert);
    ATOMS.atomClusterSettingSslPrivateKey = make_atom(env, kAtomClusterSettingSslPrivateKey);
    
    cassandra_data* data = static_cast<cassandra_data*>(enif_alloc(sizeof(cassandra_data)));
    data->cluster = cass_cluster_new();
    data->defaultConsistencyLevel = CASS_CONSISTENCY_ONE;
    
    open_resources(env, data);
    
    *priv_data = data;
    return 0;
}

void on_nif_unload(ErlNifEnv* env, void* priv_data)
{
    cassandra_data* data = static_cast<cassandra_data*>(priv_data);
    
    if(data->cluster)
        cass_cluster_free(data->cluster);
    
    enif_free(data);
}

int on_nif_upgrade(ErlNifEnv* env, void** priv, void** old_priv, ERL_NIF_TERM info)
{
    cassandra_data* old_data = static_cast<cassandra_data*>(*old_priv);
    
    cassandra_data* data = static_cast<cassandra_data*>(enif_alloc(sizeof(cassandra_data)));
    data->cluster = old_data->cluster;
    data->defaultConsistencyLevel = old_data->defaultConsistencyLevel;
    open_resources(env, data);
    
    old_data->cluster = NULL;
    *priv = data;
    
    return 0;
}

static ErlNifFunc nif_funcs[] =
{
    //CassCluster
    
    {"nif_cass_cluster_set_options", 1, nif_cass_cluster_set_options},
    
    //CassSession
    
    {"nif_cass_session_new", 0, nif_cass_session_new},
    {"nif_cass_session_connect", 2, nif_cass_session_connect},
    {"nif_cass_session_connect_keyspace", 3, nif_cass_session_connect},
    {"nif_cass_session_close", 1, nif_cass_session_close},
    {"nif_cass_session_prepare", 3, nif_cass_session_prepare},
    
    {"nif_cass_prepared_bind", 1, nif_cass_prepared_bind},
    {"nif_cass_statement_new", 1, nif_cass_statement_new},
    {"nif_cass_statement_new", 2, nif_cass_statement_new},
    {"nif_cass_statement_bind_parameters", 3, nif_cass_statement_bind_parameters},
    {"nif_cass_session_execute", 4, nif_cass_session_execute},
    {"nif_cass_session_execute_batch", 5, nif_cass_session_execute_batch},
    {"nif_cass_session_get_metrics", 1, nif_cass_session_get_metrics},
    
    //CassUuidGen
    
    {"nif_cass_uuid_gen_new", 0, nif_cass_uuid_gen_new},
    {"nif_cass_uuid_gen_time", 1, nif_cass_uuid_gen_time},
    {"nif_cass_uuid_gen_random", 1, nif_cass_uuid_gen_random},
    {"nif_cass_uuid_gen_from_time", 2, nif_cass_uuid_gen_from_time},
    
    //CassUuid
    
    {"nif_cass_uuid_min_from_time", 1, nif_cass_uuid_min_from_time},
    {"nif_cass_uuid_max_from_time", 1, nif_cass_uuid_max_from_time},
    {"nif_cass_uuid_timestamp", 1, nif_cass_uuid_timestamp},
    {"nif_cass_uuid_version", 1, nif_cass_uuid_version}
    
};

ERL_NIF_INIT(erlcass, nif_funcs, on_nif_load, NULL, on_nif_upgrade, on_nif_unload)

