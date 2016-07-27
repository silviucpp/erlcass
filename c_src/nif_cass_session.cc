//
//  nif_cass_session.cpp
//  erlcass
//
//  Created by silviu on 5/8/15.
//
//

#include "nif_cass_session.h"
#include "nif_cass_prepared.h"
#include "nif_cass_statement.h"
#include "data_conversion.h"
#include "metadata.h"
#include "nif_utils.h"
#include <string.h>

#define UINT64_METRIC(Name, Property) enif_make_tuple2(env, make_atom(env, Name), enif_make_uint64(env, Property))
#define DOUBLE_METRIC(Name, Property) enif_make_tuple2(env, make_atom(env, Name), enif_make_double(env, Property))

class CassBatchScope
{
public:
    
    CassBatchScope(CassBatch* batch) : batch_(batch) {}
    ~CassBatchScope() {cass_batch_free(batch_);}
    
    CassBatch* get() const {return batch_;}
    
private:
    
    CassBatch* batch_;
};

struct enif_cass_session
{
    CassSession* session;
};

struct callback_info
{
    ErlNifEnv *env;
    ErlNifPid pid;
    ERL_NIF_TERM arguments;
};

struct callback_statement_info
{
    ErlNifEnv *env;
    ErlNifPid pid;
    ERL_NIF_TERM arguments;
    ErlNifResourceType* prepared_res;
    CassConsistency cl;
    CassConsistency serial_cl;
    CassSession* session;
};

callback_info* callback_info_alloc(ErlNifEnv* env, const ErlNifPid& pid, ERL_NIF_TERM arg)
{
    callback_info* callback = static_cast<callback_info*>(enif_alloc(sizeof(callback_info)));
    callback->pid = pid;
    callback->env = enif_alloc_env();
    callback->arguments = enif_make_copy(callback->env, arg);
    return callback;
}

callback_info* callback_info_alloc(ErlNifEnv* env, ERL_NIF_TERM arg)
{
    ErlNifPid pid;

    if(enif_self(env, &pid) == NULL)
        return NULL;

    return callback_info_alloc(env, pid, arg);
}

void callback_info_free(callback_info* cb)
{
    if(cb->env)
        enif_free_env(cb->env);

    enif_free(cb);
}

void nif_cass_session_free(ErlNifEnv* env, void* obj)
{
    enif_cass_session *enif_session = static_cast<enif_cass_session*>(obj);
    
    if(enif_session->session != NULL)
        cass_session_free(enif_session->session);
}

void on_session_connect(CassFuture* future, void* user_data)
{
    callback_info* cb = static_cast<callback_info*>(user_data);
    
    ERL_NIF_TERM result;
    
    if (cass_future_error_code(future) != CASS_OK)
        result = cass_future_error_to_nif_term(cb->env, future);
    else
        result = ATOMS.atomOk;
    
    enif_send(NULL, &cb->pid, cb->env, enif_make_tuple2(cb->env, ATOMS.atomSessionConnected, enif_make_tuple2(cb->env, result, cb->arguments)));
    callback_info_free(cb);
}

void on_session_closed(CassFuture* future, void* user_data)
{
    callback_info* cb = static_cast<callback_info*>(user_data);
    
    ERL_NIF_TERM result;
    
    if (cass_future_error_code(future) != CASS_OK)
        result = cass_future_error_to_nif_term(cb->env, future);
    else
        result = ATOMS.atomOk;
    
    enif_send(NULL, &cb->pid, cb->env, enif_make_tuple3(cb->env, ATOMS.atomSessionClosed, cb->arguments, result));
    callback_info_free(cb);
}

void on_statement_prepared(CassFuture* future, void* user_data)
{
    callback_statement_info* cb = static_cast<callback_statement_info*>(user_data);
    ERL_NIF_TERM result;
    
    if (cass_future_error_code(future) != CASS_OK)
    {
        result = cass_future_error_to_nif_term(cb->env, future);
    }
    else
    {
        const CassPrepared* prep = cass_future_get_prepared(future);
        
        ERL_NIF_TERM term = nif_cass_prepared_new(cb->env, cb->prepared_res, prep, cb->cl, cb->serial_cl);
        
        if(enif_is_tuple(cb->env, term))
        {
            cass_prepared_free(prep);
            result = term;
        }
        else
        {
            result = enif_make_tuple2(cb->env, ATOMS.atomOk, term);
        }
    }
    
    enif_send(NULL, &cb->pid, cb->env, enif_make_tuple3(cb->env, ATOMS.atomPreparedStatementResult, result, cb->arguments));
    enif_free_env(cb->env);
    
    enif_free(cb);
}

void on_statement_executed(CassFuture* future, void* user_data)
{
    callback_info* cb = static_cast<callback_info*>(user_data);
    ERL_NIF_TERM result;
    
    if (cass_future_error_code(future) != CASS_OK)
    {
        result = cass_future_error_to_nif_term(cb->env, future);
    }
    else
    {
        const CassResult* cassResult = cass_future_get_result(future);
        result = enif_make_tuple2(cb->env, ATOMS.atomOk, cass_result_to_erlang_term(cb->env, cassResult));
        cass_result_free(cassResult);
    }
    
    enif_send(NULL, &cb->pid, cb->env, enif_make_tuple3(cb->env, ATOMS.atomExecuteStatementResult, cb->arguments, result));
    callback_info_free(cb);
}

//CassSession

ERL_NIF_TERM nif_cass_session_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    
    enif_cass_session *enif_session = static_cast<enif_cass_session*>(enif_alloc_resource(data->resCassSession, sizeof(enif_cass_session)));
    
    if(enif_session == NULL)
        return make_error(env, "enif_alloc_resource failed");

    enif_session->session = cass_session_new();
    
    ERL_NIF_TERM term = enif_make_resource(env, enif_session);
    enif_release_resource(enif_session);
    
    return enif_make_tuple2(env, ATOMS.atomOk, term);
}

ERL_NIF_TERM nif_cass_session_connect(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    
    enif_cass_session* enif_session = NULL;
    
    if(!enif_get_resource(env, argv[0], data->resCassSession, (void**) &enif_session))
        return enif_make_badarg(env);
    
    ErlNifBinary keyspace;
    memset(&keyspace, 0, sizeof(keyspace));
    
    if(argc == 3)
    {
        if(!get_bstring(env, argv[2], &keyspace))
            return enif_make_badarg(env);
    }
    
    callback_info* callback = callback_info_alloc(env, argv[1]);

    if(callback == NULL)
        return make_error(env, "failed to get the parent pid");
    
    CassFuture* future;
    
    if(!keyspace.size)
        future = cass_session_connect(enif_session->session, data->cluster);
    else
        future = cass_session_connect_keyspace_n(enif_session->session, data->cluster, BIN_TO_STR(keyspace.data), keyspace.size);
    
    CassError error = cass_future_set_callback(future, on_session_connect, callback);
    
    cass_future_free(future);
    return cass_error_to_nif_term(env, error);
}

ERL_NIF_TERM nif_cass_session_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    
    enif_cass_session * enif_session = NULL;
    
    if(!enif_get_resource(env, argv[0], data->resCassSession, (void**) &enif_session))
        return enif_make_badarg(env);
    
    ERL_NIF_TERM ref = enif_make_ref(env);
    callback_info* callback = callback_info_alloc(env, ref);
    
    if(callback == NULL)
        return make_error(env, "failed to get the parent pid");
    
    CassFuture* future = cass_session_close(enif_session->session);
    CassError error = cass_future_set_callback(future, on_session_closed, callback);
    cass_future_free(future);
    
    if(error != CASS_OK)
        return cass_error_to_nif_term(env, error);
    
    return enif_make_tuple2(env, ATOMS.atomOk, ref);
}

ERL_NIF_TERM nif_cass_session_prepare(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    
    enif_cass_session * enif_session = NULL;

    if(!enif_get_resource(env, argv[0], data->resCassSession, (void**) &enif_session))
        return enif_make_badarg(env);

    ErlNifBinary query;
    CassConsistency consistency_level = data->defaultConsistencyLevel;
    CassConsistency serial_consistency_level = CASS_CONSISTENCY_ANY;
    
    if(!parse_query_term(env, argv[1], &query, &consistency_level, &serial_consistency_level))
        return enif_make_badarg(env);
    
    ErlNifPid pid;
    
    if(enif_self(env, &pid) == NULL)
        make_error(env, "failed to get the parent pid");
    
    callback_statement_info* callback = static_cast<callback_statement_info*>(enif_alloc(sizeof(callback_info)));
    callback->pid = pid;
    callback->prepared_res = data->resCassPrepared;
    callback->env = enif_alloc_env();
    callback->arguments = enif_make_copy(callback->env, argv[2]);
    callback->cl = consistency_level;
    callback->serial_cl = serial_consistency_level;
    callback->session = enif_session->session;
    
    CassFuture* future = cass_session_prepare_n(enif_session->session, BIN_TO_STR(query.data), query.size);
    
    CassError error = cass_future_set_callback(future, on_statement_prepared, callback);
    cass_future_free(future);
    return cass_error_to_nif_term(env, error);
}

ERL_NIF_TERM nif_cass_session_execute(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    
    enif_cass_session * enif_session = NULL;
    
    if(!enif_get_resource(env, argv[0], data->resCassSession, (void**) &enif_session))
        return enif_make_badarg(env);

    CassStatement* stm = get_statement(env, data->resCassStatement, argv[1]);
    
    if(stm == NULL)
        return enif_make_badarg(env);
    
    ErlNifPid pid;
    
    if(enif_get_local_pid(env, argv[2], &pid) == 0)
        return make_error(env, "failed to get the parent pid");
    
    callback_info* callback = callback_info_alloc(env, pid, argv[3]);
    
    if(callback == NULL)
        return make_error(env, "failed to create callback info");

    CassFuture* future = cass_session_execute(enif_session->session, stm);
    CassError error = cass_future_set_callback(future, on_statement_executed, callback);
    cass_future_free(future);
    return cass_error_to_nif_term(env, error);
}

ERL_NIF_TERM nif_cass_session_execute_batch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    
    enif_cass_session * enif_session = NULL;
    
    if(!enif_get_resource(env, argv[0], data->resCassSession, (void**) &enif_session) || !enif_is_list(env, argv[2]) || !enif_is_list(env, argv[3]))
        return enif_make_badarg(env);
    
    int batch_type;
    
    if(!enif_get_int(env, argv[1], &batch_type))
        return enif_make_badarg(env);
    
    CassBatchScope batch(cass_batch_new(static_cast<CassBatchType>(batch_type)));
    
    if(!batch.get())
        return make_error(env, "failed to create the batch object");
    
    ERL_NIF_TERM statements_list = argv[2];
    ERL_NIF_TERM head;
    
    while(enif_get_list_cell(env, statements_list, &head, &statements_list))
    {
        CassStatement* stm = get_statement(env, data->resCassStatement, head);
        
        if(!stm)
            return enif_make_badarg(env);
        
        CassError error = cass_batch_add_statement(batch.get(), stm);
        
        if(error != CASS_OK)
            return cass_error_to_nif_term(env, error);
    }
    
    CassConsistency consistency_level = data->defaultConsistencyLevel;
    CassConsistency serial_consistency_level = CASS_CONSISTENCY_ANY;

    if(!parse_consistency_level_options(env, argv[3], &consistency_level, &serial_consistency_level))
         return enif_make_badarg(env);

    CassError error = cass_batch_set_consistency(batch.get(), consistency_level);
    
    if(error != CASS_OK)
        return cass_error_to_nif_term(env, error);
    
    if(serial_consistency_level != CASS_CONSISTENCY_ANY)
    {
        error = cass_batch_set_serial_consistency(batch.get(), serial_consistency_level);
        
        if(error != CASS_OK)
            return cass_error_to_nif_term(env, error);
    }
    
    ErlNifPid pid;
    
    if(enif_get_local_pid(env, argv[4], &pid) == 0)
        return make_error(env, "failed to get the parent pid");
    
    ERL_NIF_TERM tag = enif_make_ref(env);

    callback_info* callback = callback_info_alloc(env, pid, tag);
    
    if(callback == NULL)
        return make_error(env, "failed to create callback info");
    
    CassFuture* future = cass_session_execute_batch(enif_session->session, batch.get());
    error = cass_future_set_callback(future, on_statement_executed, callback);
    cass_future_free(future);
    
    if(error != CASS_OK)
        return cass_error_to_nif_term(env, error);
    
    return enif_make_tuple2(env, ATOMS.atomOk, tag);
}

ERL_NIF_TERM nif_cass_session_get_metrics(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    
    enif_cass_session * enif_session = NULL;
    
    if(!enif_get_resource(env, argv[0], data->resCassSession, (void**) &enif_session))
        return enif_make_badarg(env);
    
    CassMetrics metrics;
    cass_session_get_metrics(enif_session->session, &metrics);
    
    ERL_NIF_TERM requests = enif_make_tuple(env, 14,
                                            UINT64_METRIC("min", metrics.requests.min),
                                            UINT64_METRIC("max", metrics.requests.max),
                                            UINT64_METRIC("mean", metrics.requests.mean),
                                            UINT64_METRIC("stddev", metrics.requests.stddev),
                                            UINT64_METRIC("median", metrics.requests.median),
                                            UINT64_METRIC("percentile_75th", metrics.requests.percentile_75th),
                                            UINT64_METRIC("percentile_95th", metrics.requests.percentile_95th),
                                            UINT64_METRIC("percentile_98th", metrics.requests.percentile_98th),
                                            UINT64_METRIC("percentile_99th", metrics.requests.percentile_99th),
                                            UINT64_METRIC("percentile_999th", metrics.requests.percentile_999th),
                                            DOUBLE_METRIC("mean_rate", metrics.requests.mean_rate),
                                            DOUBLE_METRIC("one_minute_rate", metrics.requests.one_minute_rate),
                                            DOUBLE_METRIC("five_minute_rate", metrics.requests.five_minute_rate),
                                            DOUBLE_METRIC("fifteen_minute_rate", metrics.requests.fifteen_minute_rate));

    ERL_NIF_TERM stats = enif_make_tuple(env, 4,
                                            UINT64_METRIC("total_connections", metrics.stats.total_connections),
                                            UINT64_METRIC("available_connections", metrics.stats.available_connections),
                                            UINT64_METRIC("exceeded_pending_requests_water_mark", metrics.stats.exceeded_pending_requests_water_mark),
                                            UINT64_METRIC("exceeded_write_bytes_water_mark", metrics.stats.exceeded_write_bytes_water_mark));

    ERL_NIF_TERM errors = enif_make_tuple(env, 3,
                                            UINT64_METRIC("connection_timeouts", metrics.errors.connection_timeouts),
                                            UINT64_METRIC("pending_request_timeouts", metrics.errors.pending_request_timeouts),
                                            UINT64_METRIC("request_timeouts", metrics.errors.request_timeouts));
    
    ERL_NIF_TERM result = enif_make_tuple3(env,
                                           enif_make_tuple2(env, make_atom(env, "requests"), requests),
                                           enif_make_tuple2(env, make_atom(env, "stats"), stats),
                                           enif_make_tuple2(env, make_atom(env, "errors"), errors));
    
    return enif_make_tuple2(env, ATOMS.atomOk, result);
}

