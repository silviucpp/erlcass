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
        *error = Func(data->cluster, value.c_str()); \
        return true; \
    }

#define INT_SETTING(Key, Func) \
    if(enif_is_identical(term_key, Key)) \
    { \
        int value; \
        if(!enif_get_int(env, term_value, &value)) \
            return false; \
        *error = Func(data->cluster, value); \
        return true; \
    }

#define BOOL_SETTING(Key, Func) \
    if(enif_is_identical(term_key, Key)) \
    {  \
        cass_bool_t value = enif_is_identical(term_value, ATOMS.atomTrue) ? cass_true : cass_false; \
        *error = Func(data->cluster, value); \
        return true; \
    }

#define UNSIGNED_INT_SETTING(Key, Func) \
    if(enif_is_identical(term_key, Key)) \
    { \
        unsigned int value; \
        if(!enif_get_uint(env, term_value, &value)) \
            return false; \
        *error = Func(data->cluster, value); \
        return true; \
    }

#define CUSTOM_SETTING(Key, Func) \
    if(enif_is_identical(term_key, Key)) \
    { \
        return Func(env, term_value, data, error); \
    }

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

CassError internal_cass_cluster_set_token_aware_routing(CassCluster* cluster, cass_bool_t enabled)
{
    cass_cluster_set_token_aware_routing(cluster, enabled);
    return CASS_OK;
}

CassError internal_cass_cluster_set_tcp_nodelay(CassCluster* cluster, cass_bool_t enabled)
{
    cass_cluster_set_tcp_nodelay(cluster, enabled);
    return CASS_OK;
}

bool internal_cass_cluster_set_load_balance_round_robin(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data, CassError* error)
{
    cass_cluster_set_load_balance_round_robin(data->cluster);
    *error = CASS_OK;
    return true;
}

bool internal_cass_cluster_set_credentials(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data, CassError* error)
{
    const ERL_NIF_TERM *items;
    int arity;
    
    if(!enif_get_tuple(env, term_value, &arity, &items) || arity != 2)
        return false;
    
    std::string username;
    std::string pwd;
    
    if(!get_string(env, items[0], username) || !get_string(env, items[1], pwd))
        return false;
    
    cass_cluster_set_credentials(data->cluster, username.c_str(), pwd.c_str());
    *error = CASS_OK;
    return true;
}

bool internal_cass_cluster_set_load_balance_dc_aware(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data, CassError* error)
{
    const ERL_NIF_TERM *items;
    int arity;
    
    if(!enif_get_tuple(env, term_value, &arity, &items) || arity != 3)
        return false;
    
    std::string local_dc;
    unsigned int used_hosts_per_remote_dc;
    
    if(!get_string(env, items[0], local_dc) || !enif_get_uint(env, items[1], &used_hosts_per_remote_dc))
        return false;
    
    cass_bool_t allow_remote_dcs_for_local_cl = enif_is_identical(items[2], ATOMS.atomTrue) ? cass_true : cass_false;
    
    *error = cass_cluster_set_load_balance_dc_aware(data->cluster, local_dc.c_str(), used_hosts_per_remote_dc, allow_remote_dcs_for_local_cl);
    return true;
}

bool internal_cass_cluster_set_tcp_keepalive(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data, CassError* error)
{
    const ERL_NIF_TERM *items;
    int arity;
    
    if(!enif_get_tuple(env, term_value, &arity, &items) || arity != 2)
        return false;
    
    unsigned delay_sec;
    if(!enif_is_atom(env, items[0]) || !enif_get_uint(env, items[1], &delay_sec))
        return false;
    
    cass_bool_t enabled = enif_is_identical(items[0], ATOMS.atomTrue) ? cass_true : cass_false;
    cass_cluster_set_tcp_keepalive(data->cluster, enabled, delay_sec);
    
    *error = CASS_OK;
    return true;
}

bool internal_cluster_set_default_consistency_level(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data, CassError* error)
{
    int cLevel;
    
    if(!enif_get_int(env, term_value, &cLevel))
        return false;
    
    data->defaultConsistencyLevel = static_cast<CassConsistency>(cLevel);
    *error = CASS_OK;
    return true;
}

bool internal_cass_cluster_set_ssl(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data, CassError* error)
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
            return false;
        
        if(enif_is_identical(items[0], ATOMS.atomClusterSettingSslTrustedCerts))
        {
            ERL_NIF_TERM trust_list = items[1];
            ERL_NIF_TERM cert_head;
            
            std::string cert;
            
            while(enif_get_list_cell(env, trust_list, &cert_head, &trust_list))
            {
                if(!get_string(env, cert_head, cert))
                    return false;
             
                trusted_certs.push_back(cert);
            }
        }
        else if(enif_is_identical(items[0], ATOMS.atomClusterSettingSslCert))
        {
            if(!get_string(env, items[1], cert))
                return false;
        }
        else if(enif_is_identical(items[0], ATOMS.atomClusterSettingSslPrivateKey))
        {
            const ERL_NIF_TERM *pk_items;
            int pk_arity;
            
            if(!enif_get_tuple(env, items[1], &pk_arity, &pk_items) || pk_arity != 2)
                return false;
            
            if(!get_string(env, pk_items[0], private_key))
                return false;
            
            if(!get_string(env, pk_items[1], private_key_pwd))
                return false;
        }
        else if(enif_is_identical(items[0], ATOMS.atomClusterSettingSslVerifyFlags))
        {
            if(!enif_get_int(env, items[1], &verify_flags))
                return false;
        }
        else
        {
            //no valid option
            return false;
        }
    }
    
    CassSslScope ssl(cass_ssl_new());
    
    for (std::vector<std::string>::const_iterator it = trusted_certs.begin(); it != trusted_certs.end(); ++it)
    {
        *error = cass_ssl_add_trusted_cert(ssl.get(), (*it).c_str());
        
        if(*error != CASS_OK)
            return true;
    }

    if(!cert.empty())
    {
        *error = cass_ssl_set_cert(ssl.get(), cert.c_str());
        
        if(*error != CASS_OK)
            return true;
    }
    
    if(!private_key.empty())
    {
        *error = cass_ssl_set_private_key(ssl.get(), private_key.c_str(), private_key_pwd.c_str());
        
        if(*error != CASS_OK)
            return true;
    }
    
    if(verify_flags != CASS_SSL_VERIFY_NONE)
        cass_ssl_set_verify_flags(ssl.get(), verify_flags);

    cass_cluster_set_ssl(data->cluster, ssl.get());
    *error = CASS_OK;
    
    return true;
}

bool internal_cluster_set_latency_aware_routing(ErlNifEnv* env, ERL_NIF_TERM term_value, cassandra_data* data, CassError* error)
{
    if(enif_is_atom(env, term_value))
    {
        //only enable/disable
        
        cass_bool_t enabled = enif_is_identical(term_value, ATOMS.atomTrue) ? cass_true : cass_false;
        cass_cluster_set_latency_aware_routing(data->cluster, enabled);
        *error = CASS_OK;
        return true;
    }
    
    const ERL_NIF_TERM *items;
    int arity;
    
    if(!enif_get_tuple(env, term_value, &arity, &items) || arity != 2)
        return false;
    
    cass_bool_t enabled = enif_is_identical(items[0], ATOMS.atomTrue) ? cass_true : cass_false;
    cass_cluster_set_latency_aware_routing(data->cluster, enabled);
    
    //set also the settings
    
    if(!enif_get_tuple(env, items[1], &arity, &items) || arity != 5)
        return false;

    double exclusion_threshold;
    unsigned long scale_ms;
    unsigned long retry_period_ms;
    unsigned long update_rate_ms;
    unsigned long min_measured;
    
    if(!enif_get_double(env, items[0], &exclusion_threshold))
        return false;
    
    if(!enif_get_uint64(env, items[1], &scale_ms))
        return false;
    
    if(!enif_get_uint64(env, items[2], &retry_period_ms))
        return false;
    
    if(!enif_get_uint64(env, items[3], &update_rate_ms))
        return false;
    
    if(!enif_get_uint64(env, items[4], &min_measured))
        return false;
    
    cass_cluster_set_latency_aware_routing_settings(data->cluster, exclusion_threshold, scale_ms, retry_period_ms, update_rate_ms, min_measured);
    *error = CASS_OK;
    return true;
}

bool apply_cluster_settings(ErlNifEnv* env, ERL_NIF_TERM term_key, ERL_NIF_TERM term_value, cassandra_data* data, CassError* error)
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
    CUSTOM_SETTING(ATOMS.atomClusterSettingCredentials, internal_cass_cluster_set_credentials);
    CUSTOM_SETTING(ATOMS.atomClusterSettingLoadBalanceRoundRobin, internal_cass_cluster_set_load_balance_round_robin);
    CUSTOM_SETTING(ATOMS.atomClusterSettingLoadBalanceDcAware, internal_cass_cluster_set_load_balance_dc_aware);
    BOOL_SETTING(ATOMS.atomClusterSettingTokenAwareRouting, internal_cass_cluster_set_token_aware_routing);
    CUSTOM_SETTING(ATOMS.atomClusterSetringLatencyAwareRouting, internal_cluster_set_latency_aware_routing);
    BOOL_SETTING(ATOMS.atomClusterSettingTcpNodelay, internal_cass_cluster_set_tcp_nodelay);
    CUSTOM_SETTING(ATOMS.atomClusterSettingTcpKeepalive, internal_cass_cluster_set_tcp_keepalive);
    
    return false;
}

ERL_NIF_TERM nif_cass_cluster_set_options(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if(!enif_is_list(env, argv[0]))
        return enif_make_badarg(env);
 
    cassandra_data* data = (cassandra_data*) enif_priv_data(env);
    
    ERL_NIF_TERM list = argv[0];
    
    ERL_NIF_TERM head;
    const ERL_NIF_TERM *items;
    int arity;
    
    CassError error;
    
    while(enif_get_list_cell(env, list, &head, &list))
    {
        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return enif_make_badarg(env);
        
        if(!enif_is_atom(env, items[0]))
            return enif_make_badarg(env);
        
        if(!apply_cluster_settings(env, items[0], items[1], data, &error))
            return enif_make_badarg(env);
        
        if(error != CASS_OK)
            return make_error(env, cass_error_desc(error));
    }

    return ATOMS.atomOk;
}

