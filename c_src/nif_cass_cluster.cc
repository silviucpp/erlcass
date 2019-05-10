#include "nif_cass_cluster.h"
#include "erlcass.h"
#include "nif_utils.h"
#include "constants.h"
#include "macros.h"

#include <string.h>
#include <memory>

//CassCluster
struct enif_cass_cluster
{
    CassCluster* cluster;
};

#define STRING_SETTING(Key, Func) \
    if(enif_is_identical(term_key, Key)) \
    { \
        ErlNifBinary value; \
        if(!get_bstring(env, term_value, &value)) \
            return make_bad_options(env, term_option); \
        return cass_error_to_nif_term(env, Func(cluster, BIN_TO_STR(value.data), value.size)); \
    }

#define INT_SETTING(Key, Func) \
    if(enif_is_identical(term_key, Key)) \
    { \
        int value; \
        if(!enif_get_int(env, term_value, &value)) \
            return make_bad_options(env, term_option); \
        return cass_error_to_nif_term(env, Func(cluster, value)); \
    }

#define UNSIGNED_INT_SETTING(Key, Func) \
    if(enif_is_identical(term_key, Key)) \
    { \
        unsigned int value; \
        if(!enif_get_uint(env, term_value, &value)) \
            return make_bad_options(env, term_option); \
        return cass_error_to_nif_term(env, Func(cluster, value)); \
    }

#define CUSTOM_SETTING(Key, Func) \
    if(enif_is_identical(term_key, Key)) \
        return Func(env, cluster, term_option, term_value, data);


CassCluster* get_cass_cluster(ErlNifEnv* env, ErlNifResourceType* resource_type, const ERL_NIF_TERM arg)
{
    enif_cass_cluster * enif_cluster = NULL;

    if(!enif_get_resource(env, arg, resource_type, (void**) &enif_cluster))
        return NULL;

     return enif_cluster->cluster;
}


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

ERL_NIF_TERM internal_cass_cluster_set_token_aware_routing(ErlNifEnv* env, CassCluster* cluster, ERL_NIF_TERM term_option, ERL_NIF_TERM term_value, cassandra_data* data)
{
    cass_bool_t token_aware_routing;

    if(!get_boolean(term_value, &token_aware_routing))
        return make_bad_options(env, term_option);

    cass_cluster_set_token_aware_routing(cluster, token_aware_routing);
    return ATOMS.atomOk;
}

ERL_NIF_TERM internal_cass_cluster_set_tcp_nodelay(ErlNifEnv* env, CassCluster* cluster, ERL_NIF_TERM term_option, ERL_NIF_TERM term_value, cassandra_data* data)
{
    cass_bool_t nodelay;

    if(!get_boolean(term_value, &nodelay))
        return make_bad_options(env, term_option);

    cass_cluster_set_tcp_nodelay(cluster, nodelay);
    return ATOMS.atomOk;
}

ERL_NIF_TERM internal_cass_cluster_set_load_balance_round_robin(ErlNifEnv* env, CassCluster* cluster, ERL_NIF_TERM term_option, ERL_NIF_TERM term_value, cassandra_data* data)
{
    cass_cluster_set_load_balance_round_robin(cluster);
    return ATOMS.atomOk;
}

ERL_NIF_TERM internal_cass_cluster_set_credentials(ErlNifEnv* env, CassCluster* cluster, ERL_NIF_TERM term_option, ERL_NIF_TERM term_value, cassandra_data* data)
{
    const ERL_NIF_TERM *items;
    int arity;

    if(!enif_get_tuple(env, term_value, &arity, &items) || arity != 2)
        return make_bad_options(env, term_option);

    ErlNifBinary username;
    ErlNifBinary pwd;

    if(!get_bstring(env, items[0], &username) || !get_bstring(env, items[1], &pwd))
        return make_bad_options(env, term_option);

    cass_cluster_set_credentials_n(cluster, BIN_TO_STR(username.data), username.size, BIN_TO_STR(pwd.data), pwd.size);
    return ATOMS.atomOk;
}

ERL_NIF_TERM internal_cass_cluster_set_load_balance_dc_aware(ErlNifEnv* env, CassCluster* cluster, ERL_NIF_TERM term_option, ERL_NIF_TERM term_value, cassandra_data* data)
{
    const ERL_NIF_TERM *items;
    int arity;

    if(!enif_get_tuple(env, term_value, &arity, &items) || arity != 3)
        return make_bad_options(env, term_option);

    ErlNifBinary local_dc;
    unsigned int used_hosts_per_remote_dc;

    if(!get_bstring(env, items[0], &local_dc) || !enif_get_uint(env, items[1], &used_hosts_per_remote_dc))
        return make_bad_options(env, term_option);

    cass_bool_t allow_remote_dcs_for_local_cl;

    if(!get_boolean(items[2], &allow_remote_dcs_for_local_cl))
        return make_bad_options(env, term_option);

    return cass_error_to_nif_term(env, cass_cluster_set_load_balance_dc_aware_n(cluster,
                                                                                BIN_TO_STR(local_dc.data),
                                                                                local_dc.size,
                                                                                used_hosts_per_remote_dc,
                                                                                allow_remote_dcs_for_local_cl));
}

ERL_NIF_TERM internal_cass_cluster_set_tcp_keepalive(ErlNifEnv* env, CassCluster* cluster, ERL_NIF_TERM term_option, ERL_NIF_TERM term_value, cassandra_data* data)
{
    const ERL_NIF_TERM *items;
    int arity;

    if(!enif_get_tuple(env, term_value, &arity, &items) || arity != 2)
        return make_bad_options(env, term_option);

    unsigned delay_sec;
    cass_bool_t tcp_keepalive;

    if(!get_boolean(items[0], &tcp_keepalive))
        return make_bad_options(env, term_option);

    if(!enif_get_uint(env, items[1], &delay_sec))
        return make_bad_options(env, term_option);

    cass_cluster_set_tcp_keepalive(cluster, tcp_keepalive, delay_sec);
    return ATOMS.atomOk;
}

ERL_NIF_TERM internal_cluster_set_default_consistency_level(ErlNifEnv* env, CassCluster* cluster, ERL_NIF_TERM term_option, ERL_NIF_TERM term_value, cassandra_data* data)
{
    int level;

    if(!enif_get_int(env, term_value, &level))
        return make_bad_options(env, term_option);

    data->defaultConsistencyLevel = static_cast<CassConsistency>(level);
    return ATOMS.atomOk;
}

ERL_NIF_TERM internal_cluster_set_retry_policy(ErlNifEnv* env, CassCluster* cluster, ERL_NIF_TERM term_option, ERL_NIF_TERM term_value, cassandra_data* data)
{
    cass_bool_t log_enable = cass_false;
    ERL_NIF_TERM rp = term_value;

    if(enif_is_tuple(env, term_value))
    {
        const ERL_NIF_TERM *items;
        int arity;

        if(!enif_get_tuple(env, term_value, &arity, &items) || arity != 2)
            return make_bad_options(env, term_option);

        if(!get_boolean(items[1], &log_enable))
            return make_bad_options(env, term_option);

        rp = items[0];
    }

    scoped_ptr(retry_policy, CassRetryPolicy, NULL, cass_retry_policy_free);
    scoped_ptr(retry_policy_log, CassRetryPolicy, NULL, cass_retry_policy_free);

    if(enif_is_identical(rp, ATOMS.atomClusterSettingRetryPolicyDefault))
        retry_policy.reset(cass_retry_policy_default_new());
    else if(enif_is_identical(rp, ATOMS.atomClusterSettingRetryPolicyDowngradingConsistency))
        retry_policy.reset(cass_retry_policy_downgrading_consistency_new());
    else if(enif_is_identical(rp, ATOMS.atomClusterSettingRetryPolicyFallthrough))
        retry_policy.reset(cass_retry_policy_fallthrough_new());
    else
        return make_bad_options(env, term_option);

    if(log_enable)
    {
        retry_policy_log.reset(cass_retry_policy_logging_new(retry_policy.get()));
        cass_cluster_set_retry_policy(cluster, retry_policy_log.get());
    }
    else
    {
        cass_cluster_set_retry_policy(cluster, retry_policy.get());
    }

    return ATOMS.atomOk;
}

ERL_NIF_TERM internal_cass_cluster_set_ssl(ErlNifEnv* env, CassCluster* cluster, ERL_NIF_TERM term_option, ERL_NIF_TERM term_value, cassandra_data* data)
{
    if(!enif_is_list(env, term_value))
        return make_bad_options(env, term_option);

    ERL_NIF_TERM head;

    scoped_ptr(ssl, CassSsl, cass_ssl_new(), cass_ssl_free);

    while(enif_get_list_cell(env, term_value, &head, &term_value))
    {
        const ERL_NIF_TERM *items;
        int arity;

        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return make_bad_options(env, head);

        if(enif_is_identical(items[0], ATOMS.atomClusterSettingSslTrustedCerts))
        {
            ERL_NIF_TERM trust_list = items[1];
            ERL_NIF_TERM cert_head;

            ErlNifBinary cert_item;

            while(enif_get_list_cell(env, trust_list, &cert_head, &trust_list))
            {
                if(!get_bstring(env, cert_head, &cert_item))
                    return make_bad_options(env, head);

                CassError error = cass_ssl_add_trusted_cert_n(ssl.get(), BIN_TO_STR(cert_item.data), cert_item.size);

                if(error != CASS_OK)
                    return cass_error_to_nif_term(env, error);
            }
        }
        else if(enif_is_identical(items[0], ATOMS.atomClusterSettingSslCert))
        {
            ErlNifBinary cert;

            if(!get_bstring(env, items[1], &cert))
                return make_bad_options(env, head);

            CassError error = cass_ssl_set_cert_n(ssl.get(), BIN_TO_STR(cert.data), cert.size);

            if(error != CASS_OK)
                return cass_error_to_nif_term(env, error);
        }
        else if(enif_is_identical(items[0], ATOMS.atomClusterSettingSslPrivateKey))
        {
            const ERL_NIF_TERM *pk_items;
            int pk_arity;
            ErlNifBinary pk;
            ErlNifBinary pk_pwd;

            if(!enif_get_tuple(env, items[1], &pk_arity, &pk_items) || pk_arity != 2)
                return make_bad_options(env, head);

            if(!get_bstring(env, pk_items[0], &pk))
                return make_bad_options(env, head);

            if(!get_bstring(env, pk_items[1], &pk_pwd))
                return make_bad_options(env, head);

            CassError error = cass_ssl_set_private_key_n(ssl.get(), BIN_TO_STR(pk.data), pk.size, BIN_TO_STR(pk_pwd.data), pk_pwd.size);

            if(error != CASS_OK)
                return cass_error_to_nif_term(env, error);
        }
        else if(enif_is_identical(items[0], ATOMS.atomClusterSettingSslVerifyFlags))
        {
            int verify_flags;

            if(!enif_get_int(env, items[1], &verify_flags))
                return make_bad_options(env, head);

            cass_ssl_set_verify_flags(ssl.get(), verify_flags);
        }
        else
        {
            return make_bad_options(env, head);
        }
    }

    cass_cluster_set_ssl(cluster, ssl.get());

    return ATOMS.atomOk;
}

ERL_NIF_TERM internal_cluster_set_latency_aware_routing(ErlNifEnv* env, CassCluster* cluster, ERL_NIF_TERM term_option, ERL_NIF_TERM term_value, cassandra_data* data)
{
    if(enif_is_atom(env, term_value))
    {
        //only enable/disable
        cass_bool_t latency_aware_routing;

        if(!get_boolean(term_value, &latency_aware_routing))
            return make_bad_options(env, term_option);

        cass_cluster_set_latency_aware_routing(cluster, latency_aware_routing);
        return ATOMS.atomOk;
    }

    const ERL_NIF_TERM *items;
    int arity;

    if(!enif_get_tuple(env, term_value, &arity, &items) || arity != 2)
        return make_bad_options(env, term_option);

    cass_bool_t latency_aware_routing;

    if(!get_boolean(items[0], &latency_aware_routing))
        return make_bad_options(env, term_option);

    cass_cluster_set_latency_aware_routing(cluster, latency_aware_routing);

    //set also the settings

    if(!enif_get_tuple(env, items[1], &arity, &items) || arity != 5)
        return make_bad_options(env, term_option);

    double exclusion_threshold;
    unsigned long scale_ms;
    unsigned long retry_period_ms;
    unsigned long update_rate_ms;
    unsigned long min_measured;

    if(!enif_get_double(env, items[0], &exclusion_threshold))
        return make_bad_options(env, term_option);

    if(!enif_get_uint64(env, items[1], &scale_ms))
        return make_bad_options(env, term_option);

    if(!enif_get_uint64(env, items[2], &retry_period_ms))
        return make_bad_options(env, term_option);

    if(!enif_get_uint64(env, items[3], &update_rate_ms))
        return make_bad_options(env, term_option);

    if(!enif_get_uint64(env, items[4], &min_measured))
        return make_bad_options(env, term_option);

    cass_cluster_set_latency_aware_routing_settings(cluster, exclusion_threshold, scale_ms, retry_period_ms, update_rate_ms, min_measured);
    return ATOMS.atomOk;
}

ERL_NIF_TERM apply_cluster_settings(ErlNifEnv* env, CassCluster* cluster, ERL_NIF_TERM term_option, ERL_NIF_TERM term_key, ERL_NIF_TERM term_value, cassandra_data* data)
{
    CUSTOM_SETTING(ATOMS.atomClusterDefaultConsistencyLevel, internal_cluster_set_default_consistency_level);

    STRING_SETTING(ATOMS.atomClusterSettingContactPoints, cass_cluster_set_contact_points_n);
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
    CUSTOM_SETTING(ATOMS.atomClusterSettingRetryPolicy, internal_cluster_set_retry_policy);

    return make_bad_options(env, term_option);
}


void nif_cass_cluster_free(ErlNifEnv* env, void* obj)
{
    enif_cass_cluster *enif_cluster = static_cast<enif_cass_cluster*>(obj);

    if(enif_cluster->cluster != NULL)
        cass_cluster_free(enif_cluster->cluster);
}

ERL_NIF_TERM nif_cass_cluster_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    enif_cass_cluster *enif_cluster = static_cast<enif_cass_cluster*>(enif_alloc_resource(data->resCassCluster, sizeof(enif_cass_cluster)));

    if(enif_cluster == NULL)
        return make_error(env, erlcass::kFailedToAllocResourceMsg);

    enif_cluster->cluster = cass_cluster_new();

    ERL_NIF_TERM term = enif_make_resource(env, enif_cluster);
    enif_release_resource(enif_cluster);

    return enif_make_tuple2(env, ATOMS.atomOk, term);
}

ERL_NIF_TERM nif_cass_cluster_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    enif_cass_cluster * enif_cluster = NULL;

    if(!enif_get_resource(env, argv[0], data->resCassCluster, (void**) &enif_cluster))
        return make_badarg(env);

    cass_cluster_free(enif_cluster->cluster);
    enif_cluster->cluster = NULL;

    return ATOMS.atomOk;
}

ERL_NIF_TERM nif_cass_cluster_set_options(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    enif_cass_cluster * enif_cluster = NULL;

    if(!enif_get_resource(env, argv[0], data->resCassCluster, (void**) &enif_cluster))
        return make_badarg(env);

    ERL_NIF_TERM options = argv[1];

    if(!enif_is_list(env, options))
        return make_bad_options(env, options);


    ERL_NIF_TERM head;
    const ERL_NIF_TERM *items;
    int arity;

    while(enif_get_list_cell(env, options, &head, &options))
    {
        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return make_bad_options(env, head);

        ERL_NIF_TERM key = items[0];
        ERL_NIF_TERM value = items[1];

        if(!enif_is_atom(env, key))
            return make_bad_options(env, head);

        ERL_NIF_TERM result = apply_cluster_settings(env, enif_cluster->cluster, head, key, value, data);

        if(!enif_is_identical(ATOMS.atomOk, result))
            return result;
    }

    return ATOMS.atomOk;
}

void cass_log_callback(const CassLogMessage* message, void* data)
{
    if(data == NULL)
        return;

    ErlNifPid* pid = reinterpret_cast<ErlNifPid*>(data);

    ErlNifEnv* env = enif_alloc_env();

    ERL_NIF_TERM log_record = enif_make_tuple6(env,
                                               ATOMS.atomLogMsgRecord,
                                               enif_make_int(env, message->severity),
                                               make_binary(env, message->file, strlen(message->file)),
                                               enif_make_int(env, message->line),
                                               make_binary(env, message->function, strlen(message->function)),
                                               make_binary(env, message->message, strlen(message->message)));

    enif_send(NULL, pid, env, enif_make_tuple2(env, ATOMS.atomLogMessageReceived, log_record));

    enif_free_env(env);
}

ERL_NIF_TERM nif_cass_log_set_level(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int log_level;

    if(!enif_get_int(env, argv[0], &log_level))
        return make_badarg(env);

    cass_log_set_level(static_cast<CassLogLevel>(log_level));
    return ATOMS.atomOk;
}

ERL_NIF_TERM nif_cass_log_set_callback(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    if(enif_is_identical(argv[0], ATOMS.atomNull))
    {
        cass_log_set_callback(cass_log_callback, NULL);
    }
    else
    {
        if(!enif_get_local_pid(env, argv[0], &data->log_pid))
            return make_badarg(env);

        cass_log_set_callback(cass_log_callback, &data->log_pid);
    }

    return ATOMS.atomOk;
}
