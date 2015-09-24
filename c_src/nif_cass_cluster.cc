//
//  nif_cass_cluster.cpp
//  erlcass
//
//  Created by silviu on 5/8/15.
//
//

#include "nif_cass_cluster.h"
#include "erlcass.h"
#include "nif_utils.h"

#include <vector>

//CassCluster

#define STRING_SETTING(Key, Func) \
    if(enif_is_identical(term_key, Key)) \
    { \
        std::string value; \
        if(!get_string(env, term_value, value)) \
            return false; \
        return cass_error_to_nif_term(env, Func(data->cluster, value.c_str())); \
    }

#define INT_SETTING(Key, Func) \
    if(enif_is_identical(term_key, Key)) \
    { \
        int value; \
        if(!enif_get_int(env, term_value, &value)) \
            return false; \
        return cass_error_to_nif_term(env, Func(data->cluster, value)); \
    }

#define UNSIGNED_INT_SETTING(Key, Func) \
    if(enif_is_identical(term_key, Key)) \
    { \
        unsigned int value; \
        if(!enif_get_uint(env, term_value, &value)) \
            return false; \
        return cass_error_to_nif_term(env, Func(data->cluster, value)); \
    }

#define CUSTOM_SETTING(Key, Func) \
    if(enif_is_identical(term_key, Key)) \
        return Func(env, term_value, data);

class CassSslScope
{
public:
    
    CassSslScope(CassSsl* ssl) : ssl_(ssl) {}
    ~CassSslScope() {cass_ssl_free(ssl_);}
    
    CassSsl* get() const {return ssl_;}
    
private:
    
    CassSsl* ssl_;
};

CassError internal_cass_cluster_set_reconnect_wait_time(CassCluster* cluster, unsigned wait_time)
{
    cass_cluster_set_reconnect_wait_time(cluster, wait_time);
    return CASS_OK;
}

CassError internal_cass_cluster_set_connect_timeout(CassCluster* cluster, unsigned timeout)
{
    cass_cluster_set_connect_timeout(cluster, timeout);
    return CASS_OK;
}

CassError internal_cass_cluster_set_request_timeout(CassCluster* cluster, unsigned timeout)
{
    cass_cluster_set_request_timeout(cluster, timeout);
    return CASS_OK;
}

CassError internal_cass_cluster_set_connection_heartbeat_interval(CassCluster* cluster, unsigned interval)
{
    cass_cluster_set_connection_heartbeat_interval(cluster, interval);
    return CASS_OK;
}

CassError internal_cass_cluster_set_connection_idle_timeout(CassCluster* cluster, unsigned timeout)
{
    cass_cluster_set_connection_idle_timeout(cluster, timeout);
    return CASS_OK;
}

ERL_NIF_TERM internal_cass_cluster_set_token_aware_routing(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data)
{
    cass_bool_t value = enif_is_identical(term_value, ATOMS.atomTrue) ? cass_true : cass_false;
    cass_cluster_set_token_aware_routing(data->cluster, value);
    return ATOMS.atomOk;
}

ERL_NIF_TERM internal_cass_cluster_set_tcp_nodelay(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data)
{
    cass_bool_t value = enif_is_identical(term_value, ATOMS.atomTrue) ? cass_true : cass_false;
    cass_cluster_set_tcp_nodelay(data->cluster, value);
    return ATOMS.atomOk;
}

ERL_NIF_TERM internal_cass_cluster_set_load_balance_round_robin(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data)
{
    cass_cluster_set_load_balance_round_robin(data->cluster);
    return ATOMS.atomOk;
}

ERL_NIF_TERM internal_cass_cluster_set_credentials(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data)
{
    const ERL_NIF_TERM *items;
    int arity;
    
    if(!enif_get_tuple(env, term_value, &arity, &items) || arity != 2)
        return enif_make_badarg(env);
    
    std::string username;
    std::string pwd;
    
    if(!get_string(env, items[0], username) || !get_string(env, items[1], pwd))
        return enif_make_badarg(env);
    
    cass_cluster_set_credentials(data->cluster, username.c_str(), pwd.c_str());
    return ATOMS.atomOk;
}

ERL_NIF_TERM internal_cass_cluster_set_load_balance_dc_aware(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data)
{
    const ERL_NIF_TERM *items;
    int arity;
    
    if(!enif_get_tuple(env, term_value, &arity, &items) || arity != 3)
        return enif_make_badarg(env);
    
    std::string local_dc;
    unsigned int used_hosts_per_remote_dc;
    
    if(!get_string(env, items[0], local_dc) || !enif_get_uint(env, items[1], &used_hosts_per_remote_dc))
        return enif_make_badarg(env);
    
    cass_bool_t allow_remote_dcs_for_local_cl = enif_is_identical(items[2], ATOMS.atomTrue) ? cass_true : cass_false;
    
    return cass_error_to_nif_term(env,
                                  cass_cluster_set_load_balance_dc_aware(data->cluster,
                                                                         local_dc.c_str(),
                                                                         used_hosts_per_remote_dc,
                                                                         allow_remote_dcs_for_local_cl));
}

ERL_NIF_TERM internal_cass_cluster_set_tcp_keepalive(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data)
{
    const ERL_NIF_TERM *items;
    int arity;
    
    if(!enif_get_tuple(env, term_value, &arity, &items) || arity != 2)
        return enif_make_badarg(env);
    
    unsigned delay_sec;
    if(!enif_is_atom(env, items[0]) || !enif_get_uint(env, items[1], &delay_sec))
        return enif_make_badarg(env);
    
    cass_bool_t enabled = enif_is_identical(items[0], ATOMS.atomTrue) ? cass_true : cass_false;
    cass_cluster_set_tcp_keepalive(data->cluster, enabled, delay_sec);
    return ATOMS.atomOk;
}

ERL_NIF_TERM internal_cluster_set_default_consistency_level(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data)
{
    int cLevel;
    
    if(!enif_get_int(env, term_value, &cLevel))
        return enif_make_badarg(env);
    
    data->defaultConsistencyLevel = static_cast<CassConsistency>(cLevel);
    return ATOMS.atomOk;
}

ERL_NIF_TERM internal_cass_cluster_set_ssl(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data)
{
    ERL_NIF_TERM head;

    std::vector<std::string> trusted_certs;
    std::string cert;
    std::string private_key;
    std::string private_key_pwd;
    int verify_flags = CASS_SSL_VERIFY_PEER_CERT;
    
    while(enif_get_list_cell(env, term_value, &head, &term_value))
    {
        const ERL_NIF_TERM *items;
        int arity;
        
        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return enif_make_badarg(env);
        
        if(enif_is_identical(items[0], ATOMS.atomClusterSettingSslTrustedCerts))
        {
            ERL_NIF_TERM trust_list = items[1];
            ERL_NIF_TERM cert_head;
            
            std::string cert;
            
            while(enif_get_list_cell(env, trust_list, &cert_head, &trust_list))
            {
                if(!get_string(env, cert_head, cert))
                    return enif_make_badarg(env);
             
                trusted_certs.push_back(cert);
            }
        }
        else if(enif_is_identical(items[0], ATOMS.atomClusterSettingSslCert))
        {
            if(!get_string(env, items[1], cert))
                return enif_make_badarg(env);
        }
        else if(enif_is_identical(items[0], ATOMS.atomClusterSettingSslPrivateKey))
        {
            const ERL_NIF_TERM *pk_items;
            int pk_arity;
            
            if(!enif_get_tuple(env, items[1], &pk_arity, &pk_items) || pk_arity != 2)
                return enif_make_badarg(env);
            
            if(!get_string(env, pk_items[0], private_key))
                return enif_make_badarg(env);
            
            if(!get_string(env, pk_items[1], private_key_pwd))
                return enif_make_badarg(env);
        }
        else if(enif_is_identical(items[0], ATOMS.atomClusterSettingSslVerifyFlags))
        {
            if(!enif_get_int(env, items[1], &verify_flags))
                return enif_make_badarg(env);
        }
        else
        {
            //no valid option
            return enif_make_badarg(env);
        }
    }
    
    CassSslScope ssl(cass_ssl_new());
    
    for (std::vector<std::string>::const_iterator it = trusted_certs.begin(); it != trusted_certs.end(); ++it)
    {
        CassError error = cass_ssl_add_trusted_cert(ssl.get(), (*it).c_str());
        
        if(error != CASS_OK)
            return cass_error_to_nif_term(env, error);
    }

    if(!cert.empty())
    {
        CassError error = cass_ssl_set_cert(ssl.get(), cert.c_str());
        
        if(error != CASS_OK)
            return cass_error_to_nif_term(env, error);
    }
    
    if(!private_key.empty())
    {
        CassError error = cass_ssl_set_private_key(ssl.get(), private_key.c_str(), private_key_pwd.c_str());
        
        if(error != CASS_OK)
            return cass_error_to_nif_term(env, error);
    }
    
    if(verify_flags != CASS_SSL_VERIFY_NONE)
        cass_ssl_set_verify_flags(ssl.get(), verify_flags);

    cass_cluster_set_ssl(data->cluster, ssl.get());
    
    return ATOMS.atomOk;
}

ERL_NIF_TERM internal_cluster_set_latency_aware_routing(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data)
{
    if(enif_is_atom(env, term_value))
    {
        //only enable/disable
        
        cass_bool_t enabled = enif_is_identical(term_value, ATOMS.atomTrue) ? cass_true : cass_false;
        cass_cluster_set_latency_aware_routing(data->cluster, enabled);
        return ATOMS.atomOk;
    }
    
    const ERL_NIF_TERM *items;
    int arity;
    
    if(!enif_get_tuple(env, term_value, &arity, &items) || arity != 2)
        return enif_make_badarg(env);
    
    cass_bool_t enabled = enif_is_identical(items[0], ATOMS.atomTrue) ? cass_true : cass_false;
    cass_cluster_set_latency_aware_routing(data->cluster, enabled);
    
    //set also the settings
    
    if(!enif_get_tuple(env, items[1], &arity, &items) || arity != 5)
        return enif_make_badarg(env);

    double exclusion_threshold;
    unsigned long scale_ms;
    unsigned long retry_period_ms;
    unsigned long update_rate_ms;
    unsigned long min_measured;
    
    if(!enif_get_double(env, items[0], &exclusion_threshold))
        return enif_make_badarg(env);
    
    if(!enif_get_uint64(env, items[1], &scale_ms))
        return enif_make_badarg(env);
    
    if(!enif_get_uint64(env, items[2], &retry_period_ms))
        return enif_make_badarg(env);
    
    if(!enif_get_uint64(env, items[3], &update_rate_ms))
        return enif_make_badarg(env);
    
    if(!enif_get_uint64(env, items[4], &min_measured))
        return enif_make_badarg(env);
    
    cass_cluster_set_latency_aware_routing_settings(data->cluster, exclusion_threshold, scale_ms, retry_period_ms, update_rate_ms, min_measured);
    return ATOMS.atomOk;
}

ERL_NIF_TERM apply_cluster_settings(ErlNifEnv* env, ERL_NIF_TERM term_key, ERL_NIF_TERM term_value, cassandra_data* data)
{
    CUSTOM_SETTING(ATOMS.atomClusterDefaultConsistencyLevel, internal_cluster_set_default_consistency_level);
    
    STRING_SETTING(ATOMS.atomClusterSettingContactPoints, cass_cluster_set_contact_points);
    INT_SETTING(ATOMS.atomClusterSettingPort, cass_cluster_set_port);
    CUSTOM_SETTING(ATOMS.atomClusterSettingSsl, internal_cass_cluster_set_ssl);
    INT_SETTING(ATOMS.atomClusterSettingProtocolVersion, cass_cluster_set_protocol_version);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingNumThreadsIo, cass_cluster_set_num_threads_io);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingQueueSizeIo, cass_cluster_set_queue_size_io);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingQueueSizeEvent, cass_cluster_set_queue_size_event);
    //@todo: implement cass_cluster_set_queue_size_log
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingCoreConnectionsPerHost, cass_cluster_set_core_connections_per_host);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingMaxConnectionsPerHost, cass_cluster_set_max_connections_per_host);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingReconnectWaitTime, internal_cass_cluster_set_reconnect_wait_time);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingMaxConcurrentCreation, cass_cluster_set_max_concurrent_creation);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingMaxConcurrentRequestsThreshold, cass_cluster_set_max_concurrent_requests_threshold);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingMaxRequestsPerFlush, cass_cluster_set_max_requests_per_flush);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingWriteBytesHighWaterMark, cass_cluster_set_write_bytes_high_water_mark);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingWriteBytesLowWaterMark, cass_cluster_set_write_bytes_low_water_mark);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingPendingRequestsHighWaterMark, cass_cluster_set_pending_requests_high_water_mark);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingPendingRequestsLowWaterMark, cass_cluster_set_pending_requests_low_water_mark);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingConnectTimeout, internal_cass_cluster_set_connect_timeout);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingRequestTimeout, internal_cass_cluster_set_request_timeout);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingHeartbeatInterval, internal_cass_cluster_set_connection_heartbeat_interval);
    UNSIGNED_INT_SETTING(ATOMS.atomClusterSettingIdleTimeout, internal_cass_cluster_set_connection_idle_timeout);
    CUSTOM_SETTING(ATOMS.atomClusterSettingCredentials, internal_cass_cluster_set_credentials);
    CUSTOM_SETTING(ATOMS.atomClusterSettingLoadBalanceRoundRobin, internal_cass_cluster_set_load_balance_round_robin);
    CUSTOM_SETTING(ATOMS.atomClusterSettingLoadBalanceDcAware, internal_cass_cluster_set_load_balance_dc_aware);
    CUSTOM_SETTING(ATOMS.atomClusterSettingTokenAwareRouting, internal_cass_cluster_set_token_aware_routing);
    CUSTOM_SETTING(ATOMS.atomClusterSetringLatencyAwareRouting, internal_cluster_set_latency_aware_routing);
    CUSTOM_SETTING(ATOMS.atomClusterSettingTcpNodelay, internal_cass_cluster_set_tcp_nodelay);
    CUSTOM_SETTING(ATOMS.atomClusterSettingTcpKeepalive, internal_cass_cluster_set_tcp_keepalive);
    
    return enif_make_badarg(env);
}

ERL_NIF_TERM nif_cass_cluster_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    
    if(data->cluster)
        return make_error(env, "Cluster object already exist");
    
    data->cluster = cass_cluster_new();
    
    if(data->cluster == NULL)
        return make_error(env, "Failed to create the cluster object");
    
    return ATOMS.atomOk;
}

ERL_NIF_TERM nif_cass_cluster_set_options(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if(!enif_is_list(env, argv[0]))
        return enif_make_badarg(env);
 
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    
    ERL_NIF_TERM list = argv[0];
    
    ERL_NIF_TERM head;
    const ERL_NIF_TERM *items;
    int arity;
    
    while(enif_get_list_cell(env, list, &head, &list))
    {
        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return enif_make_badarg(env);
        
        if(!enif_is_atom(env, items[0]))
            return enif_make_badarg(env);
        
        ERL_NIF_TERM result = apply_cluster_settings(env, items[0], items[1], data);
        
        if(!enif_is_identical(ATOMS.atomOk, result))
            return result;
    }

    return ATOMS.atomOk;
}

void cass_log_callback(const CassLogMessage* message, void* data)
{
    ErlNifPid* pid = reinterpret_cast<ErlNifPid*>(data);
    
    ErlNifEnv* env = enif_alloc_env();
    
    const char* severity_str = cass_log_level_string(message->severity);
    
    ERL_NIF_TERM log_record = enif_make_tuple8(env,
                                               ATOMS.atomLogMsgRecord,
                                               enif_make_long(env, message->time_ms),
                                               enif_make_int(env, message->severity),
                                               make_binary(env, severity_str, strlen(severity_str)),
                                               make_binary(env, message->file, strlen(message->file)),
                                               enif_make_int(env, message->line),
                                               make_binary(env, message->function, strlen(message->function)),
                                               make_binary(env, message->message, strlen(message->message)));
    
    enif_send(NULL, pid, env, enif_make_tuple2(env, ATOMS.atomLogMessageReceived, log_record));
    
    enif_free_env(env);
}

ERL_NIF_TERM nif_cass_log_set_level_and_callback(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    
    if(data->cluster)
        return make_error(env, "Log level and callback should be set before any other function");
    
    int log_level;
    
    if(!enif_get_int(env, argv[0], &log_level))
        return enif_make_badarg(env);

    if(!enif_get_local_pid(env, argv[1], &data->log_pid))
        return enif_make_badarg(env);

    cass_log_set_level(static_cast<CassLogLevel>(log_level));
    cass_log_set_callback(cass_log_callback, &data->log_pid);
    
    return ATOMS.atomOk;
}
