#include "nif_cass_session.h"
#include "nif_cass_prepared.h"
#include "nif_cass_statement.h"
#include "data_conversion.h"
#include "nif_utils.h"
#include "constants.h"
#include "macros.h"
#include "logger.hpp"
#include "cassandra.h"

#include <string.h>
#include <memory>

struct enif_cass_session
{
    CassSession* session;
};

struct callback_info
{
    ErlNifEnv *env;
    ErlNifPid pid;
    ERL_NIF_TERM arguments;
    bool fire_and_forget;
    CassStatement* paged_statment;
};

struct callback_statement_info
{
    ErlNifEnv *env;
    ErlNifPid pid;
    ERL_NIF_TERM arguments;
    ErlNifResourceType* prepared_res;
    ConsistencyLevelOptions consistency;
    CassSession* session;
};

callback_info* callback_info_alloc(ErlNifEnv* env, const ErlNifPid& pid, ERL_NIF_TERM arg)
{
    callback_info* callback = static_cast<callback_info*>(enif_alloc(sizeof(callback_info)));
    callback->pid = pid;
    callback->env = enif_alloc_env();
    callback->arguments = enif_make_copy(callback->env, arg);
    callback->fire_and_forget = false;
    callback->paged_statment = NULL;
    return callback;
}

callback_info* callback_info_alloc(ErlNifEnv* env, ERL_NIF_TERM identifier)
{
    callback_info* callback = static_cast<callback_info*>(enif_alloc(sizeof(callback_info)));
    callback->env = enif_alloc_env();
    callback->arguments = enif_make_copy(callback->env, identifier);
    callback->fire_and_forget = true;
    callback->paged_statment = NULL;
    return callback;
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

    enif_send(NULL, &cb->pid, cb->env, enif_make_tuple3(cb->env, ATOMS.atomSessionConnected, cb->arguments, result));
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

        ERL_NIF_TERM term = nif_cass_prepared_new(cb->env, cb->prepared_res, prep, cb->consistency);

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

    cass_bool_t has_more_pages = cass_false;

    if(cb->fire_and_forget)
    {
        // we only log the response in case it's an error

        if (cass_future_error_code(future) != CASS_OK)
        {
            ErlNifBinary query_id;
            const char* message;
            size_t message_length;
            cass_future_error_message(future, &message, &message_length);

            if(get_bstring(cb->env, cb->arguments, &query_id))
                datastax::internal::Logger::log(CASS_LOG_ERROR, __FILE__, __LINE__, __FUNCTION__, "'%.*s' -> %.*s", static_cast<int>(query_id.size), BIN_TO_STR(query_id.data), static_cast<int>(message_length), message);
            else
                datastax::internal::Logger::log(CASS_LOG_ERROR, __FILE__, __LINE__, __FUNCTION__, "'unknown identifier' -> %.*s", static_cast<int>(message_length), message);
        }
    }
    else
    {
        ERL_NIF_TERM result;

        if (cass_future_error_code(future) != CASS_OK)
        {
            result = cass_future_error_to_nif_term(cb->env, future);
        }
        else
        {
            const CassResult* cassResult = cass_future_get_result(future);
            result = cass_result_to_erlang_term(cb->env, cassResult);
            if (cb->paged_statment != NULL) {
                /* Check to see if there are more pages remaining for this result */
                has_more_pages = cass_result_has_more_pages(cassResult);

                if (has_more_pages) {
                    /* If there are more pages we need to set the position for the next execute */
                    cass_statement_set_paging_state(cb->paged_statment, cassResult);
                }
            }
            cass_result_free(cassResult);
        }

        if (cb->paged_statment != NULL) {
            if (!has_more_pages) {
                enif_send(NULL, &cb->pid, cb->env, enif_make_tuple3(cb->env, ATOMS.atomPagedExecuteStatementResult, cb->arguments, result));
            } else {
                enif_send(NULL, &cb->pid, cb->env, enif_make_tuple3(cb->env, ATOMS.atomPagedExecuteStatementResultHasMore, cb->arguments, result));
            }
        } else {
            enif_send(NULL, &cb->pid, cb->env, enif_make_tuple3(cb->env, ATOMS.atomExecuteStatementResult, cb->arguments, result));
        }
    }

    callback_info_free(cb);
}

// CassSession

ERL_NIF_TERM nif_cass_session_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    enif_cass_session *enif_session = static_cast<enif_cass_session*>(enif_alloc_resource(data->resCassSession, sizeof(enif_cass_session)));

    if(enif_session == NULL)
        return make_error(env, erlcass::kFailedToAllocResourceMsg);

    enif_session->session = cass_session_new();

    ERL_NIF_TERM term = enif_make_resource(env, enif_session);
    enif_release_resource(enif_session);

    return enif_make_tuple2(env, ATOMS.atomOk, term);
}

ERL_NIF_TERM nif_cass_session_connect(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    enif_cass_session* enif_session = NULL;
    ErlNifBinary keyspace;
    ErlNifPid pid;

    if(!enif_get_resource(env, argv[0], data->resCassSession, reinterpret_cast<void**>(&enif_session)))
        return make_badarg(env);

    if(!enif_get_local_pid(env, argv[1], &pid))
        return make_badarg(env);

    if(argc == 3 && !get_bstring(env, argv[2], &keyspace))
        return make_badarg(env);

    callback_info* callback = callback_info_alloc(env, pid, argv[1]);

    if(callback == NULL)
        return make_error(env, erlcass::kFailedToCreateCallbackInfoMsg);

    CassFuture* future;

    if(argc == 3)
        future = cass_session_connect_keyspace_n(enif_session->session, data->cluster, BIN_TO_STR(keyspace.data), keyspace.size);
    else
        future = cass_session_connect(enif_session->session, data->cluster);

    CassError error = cass_future_set_callback(future, on_session_connect, callback);

    cass_future_free(future);
    return cass_error_to_nif_term(env, error);
}

ERL_NIF_TERM nif_cass_session_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    enif_cass_session * enif_session = NULL;
    ErlNifPid pid;

    if(!enif_get_resource(env, argv[0], data->resCassSession, reinterpret_cast<void**>(&enif_session)))
        return make_badarg(env);

    if(enif_get_local_pid(env, argv[1], &pid) == 0)
        return make_badarg(env);

    callback_info* callback = callback_info_alloc(env, pid, argv[1]);

    if(callback == NULL)
        return make_error(env, erlcass::kFailedToCreateCallbackInfoMsg);

    CassFuture* future = cass_session_close(enif_session->session);
    CassError error = cass_future_set_callback(future, on_session_closed, callback);
    cass_future_free(future);

    if(error != CASS_OK)
        return cass_error_to_nif_term(env, error);

    return ATOMS.atomOk;
}

ERL_NIF_TERM nif_cass_session_prepare(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    enif_cass_session * enif_session = NULL;
    ErlNifPid pid;

    if(!enif_get_resource(env, argv[0], data->resCassSession, reinterpret_cast<void**>(&enif_session)))
        return make_badarg(env);

    if(!enif_get_local_pid(env, argv[1], &pid))
        return make_badarg(env);

    QueryTerm q(ConsistencyLevelOptions(data->defaultConsistencyLevel, CASS_CONSISTENCY_ANY));

    ERL_NIF_TERM parse_result = parse_query_term(env, argv[2], &q);

    if(!enif_is_identical(ATOMS.atomOk, parse_result))
        return parse_result;

    callback_statement_info* callback = static_cast<callback_statement_info*>(enif_alloc(sizeof(callback_statement_info)));
    callback->pid = pid;
    callback->prepared_res = data->resCassPrepared;
    callback->env = enif_alloc_env();
    callback->arguments = enif_make_copy(callback->env, argv[3]);
    callback->consistency = q.consistency;
    callback->session = enif_session->session;

    CassFuture* future = cass_session_prepare_n(enif_session->session, BIN_TO_STR(q.query.data), q.query.size);

    CassError error = cass_future_set_callback(future, on_statement_prepared, callback);
    cass_future_free(future);
    return cass_error_to_nif_term(env, error);
}

ERL_NIF_TERM nif_cass_session_execute(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    enif_cass_session * enif_session = NULL;

    if(!enif_get_resource(env, argv[1], data->resCassSession, reinterpret_cast<void**>(&enif_session)))
        return make_badarg(env);

    CassStatement* stm = get_statement(env, data->resCassStatement, argv[2]);

    if(stm == NULL)
        return make_badarg(env);

    callback_info* callback = NULL;

    if(!enif_is_identical(ATOMS.atomNull, argv[3]))
    {
        ErlNifPid pid;

        if(enif_get_local_pid(env, argv[3], &pid) == 0)
            return make_badarg(env);

        callback = callback_info_alloc(env, pid, argv[4]);
    }
    else
    {
        if(!enif_is_binary(env, argv[0]))
            return make_badarg(env);

        callback = callback_info_alloc(env, argv[0]);
    }

    if(callback == NULL)
        return make_error(env, erlcass::kFailedToCreateCallbackInfoMsg);

    CassFuture* future = cass_session_execute(enif_session->session, stm);
    CassError error = cass_future_set_callback(future, on_statement_executed, callback);
    cass_future_free(future);
    return cass_error_to_nif_term(env, error);
}

ERL_NIF_TERM nif_cass_session_execute_paged(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    enif_cass_session * enif_session = NULL;

    if(!enif_get_resource(env, argv[1], data->resCassSession, (void**) &enif_session))
        return make_badarg(env);

    CassStatement* stm = get_statement(env, data->resCassStatement, argv[2]);

    if(stm == NULL)
        return make_badarg(env);

    callback_info* callback = NULL;

    if(!enif_is_identical(ATOMS.atomNull, argv[3]))
    {
        ErlNifPid pid;

        if(enif_get_local_pid(env, argv[3], &pid) == 0)
            return make_badarg(env);

        callback = callback_info_alloc(env, pid, argv[4]);
    }
    else
    {
        return make_badarg(env);
    }

    if(callback == NULL)
        return make_error(env, erlcass::kFailedToCreateCallbackInfoMsg);

    callback->paged_statment = stm;

    CassFuture* future = cass_session_execute(enif_session->session, stm);
    CassError error = cass_future_set_callback(future, on_statement_executed, callback);
    cass_future_free(future);
    return cass_error_to_nif_term(env, error);
}

ERL_NIF_TERM nif_cass_session_execute_batch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    enif_cass_session * enif_session = NULL;
    int batch_type;
    ErlNifPid pid;

    if(!enif_get_resource(env, argv[0], data->resCassSession, reinterpret_cast<void**>(&enif_session)))
        return make_badarg(env);

    if(!enif_get_int(env, argv[1], &batch_type))
        return make_badarg(env);

    if(!enif_is_list(env, argv[2]) || !enif_is_list(env, argv[3]))
        return make_badarg(env);

    if(enif_get_local_pid(env, argv[4], &pid) == 0)
        return make_badarg(env);

    scoped_ptr(batch, CassBatch, cass_batch_new(static_cast<CassBatchType>(batch_type)), cass_batch_free);

    if(!batch.get())
        return make_error(env, erlcass::kFailedToCreateBatchObjectMsg);

    ERL_NIF_TERM statements_list = argv[2];
    ERL_NIF_TERM head;

    while(enif_get_list_cell(env, statements_list, &head, &statements_list))
    {
        CassStatement* stm = get_statement(env, data->resCassStatement, head);

        if(!stm)
            return make_badarg(env);

        CassError error = cass_batch_add_statement(batch.get(), stm);

        if(error != CASS_OK)
            return cass_error_to_nif_term(env, error);
    }

    ConsistencyLevelOptions cls(data->defaultConsistencyLevel, CASS_CONSISTENCY_ANY);

    ERL_NIF_TERM parse_result = parse_consistency_level_options(env, argv[3], &cls);

    if(!enif_is_identical(ATOMS.atomOk, parse_result))
         return parse_result;

    CassError error = cass_batch_set_consistency(batch.get(), cls.cl);

    if(error != CASS_OK)
        return cass_error_to_nif_term(env, error);

    if(cls.serial_cl != CASS_CONSISTENCY_ANY)
    {
        error = cass_batch_set_serial_consistency(batch.get(), cls.serial_cl);

        if(error != CASS_OK)
            return cass_error_to_nif_term(env, error);
    }

    ERL_NIF_TERM tag = enif_make_ref(env);

    callback_info* callback = callback_info_alloc(env, pid, tag);

    if(callback == NULL)
        return make_error(env, erlcass::kFailedToCreateCallbackInfoMsg);

    CassFuture* future = cass_session_execute_batch(enif_session->session, batch.get());
    error = cass_future_set_callback(future, on_statement_executed, callback);
    cass_future_free(future);

    if(error != CASS_OK)
        return cass_error_to_nif_term(env, error);

    return enif_make_tuple2(env, ATOMS.atomOk, tag);
}

ERL_NIF_TERM metric_uint64(ErlNifEnv* env, const char* name, cass_uint64_t prop)
{
    return enif_make_tuple2(env, make_atom(env, name), enif_make_uint64(env, prop));
}

ERL_NIF_TERM metric_double(ErlNifEnv* env, const char* name, cass_double_t prop)
{
    return enif_make_tuple2(env, make_atom(env, name), enif_make_double(env, prop));
}

ERL_NIF_TERM nif_cass_session_get_metrics(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    enif_cass_session * enif_session = NULL;

    if(!enif_get_resource(env, argv[0], data->resCassSession, reinterpret_cast<void**>(&enif_session)))
        return make_badarg(env);

    CassMetrics metrics;
    cass_session_get_metrics(enif_session->session, &metrics);

    ERL_NIF_TERM requests = enif_make_tuple(env, 14,
                                            metric_uint64(env, "min", metrics.requests.min),
                                            metric_uint64(env, "max", metrics.requests.max),
                                            metric_uint64(env, "mean", metrics.requests.mean),
                                            metric_uint64(env, "stddev", metrics.requests.stddev),
                                            metric_uint64(env, "median", metrics.requests.median),
                                            metric_uint64(env, "percentile_75th", metrics.requests.percentile_75th),
                                            metric_uint64(env, "percentile_95th", metrics.requests.percentile_95th),
                                            metric_uint64(env, "percentile_98th", metrics.requests.percentile_98th),
                                            metric_uint64(env, "percentile_99th", metrics.requests.percentile_99th),
                                            metric_uint64(env, "percentile_999th", metrics.requests.percentile_999th),
                                            metric_double(env, "mean_rate", metrics.requests.mean_rate),
                                            metric_double(env, "one_minute_rate", metrics.requests.one_minute_rate),
                                            metric_double(env, "five_minute_rate", metrics.requests.five_minute_rate),
                                            metric_double(env, "fifteen_minute_rate", metrics.requests.fifteen_minute_rate));

    ERL_NIF_TERM stats = enif_make_tuple(env, 1,
                                            metric_uint64(env, "total_connections", metrics.stats.total_connections));

    ERL_NIF_TERM errors = enif_make_tuple(env, 3,
                                            metric_uint64(env, "connection_timeouts", metrics.errors.connection_timeouts),
                                            metric_uint64(env, "pending_request_timeouts", metrics.errors.pending_request_timeouts),
                                            metric_uint64(env, "request_timeouts", metrics.errors.request_timeouts));

    ERL_NIF_TERM result = enif_make_tuple3(env,
                                           enif_make_tuple2(env, make_atom(env, "requests"), requests),
                                           enif_make_tuple2(env, make_atom(env, "stats"), stats),
                                           enif_make_tuple2(env, make_atom(env, "errors"), errors));

    return enif_make_tuple2(env, ATOMS.atomOk, result);
}

ERL_NIF_TERM nif_cass_session_get_schema_metadata(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    enif_cass_session* enif_session = NULL;

    if(!enif_get_resource(env, argv[0], data->resCassSession, reinterpret_cast<void**>(&enif_session)))
        return enif_make_badarg(env);

    scoped_ptr(schema_meta, const CassSchemaMeta, cass_session_get_schema_meta(enif_session->session), cass_schema_meta_free);

    if(argc == 1)
        return enif_make_tuple2(env, ATOMS.atomOk, cass_schema_meta_fields_to_erlang_term(env, schema_meta.get()));

    ErlNifBinary keyspace;

    if(!get_bstring(env, argv[1], &keyspace))
        return make_badarg(env);

    const CassKeyspaceMeta* keyspace_meta = cass_schema_meta_keyspace_by_name_n(schema_meta.get(), BIN_TO_STR(keyspace.data), keyspace.size);

    if (keyspace_meta == NULL)
        return make_error(env, erlcass::kUnknownKeyspace);

    if(argc == 2)
        return enif_make_tuple2(env, ATOMS.atomOk, cass_keyspace_meta_fields_to_erlang_term(env, keyspace_meta));

    ErlNifBinary table;

    if(!get_bstring(env, argv[2], &table))
        return make_badarg(env);

    const CassTableMeta* table_meta = cass_keyspace_meta_table_by_name_n(keyspace_meta, BIN_TO_STR(table.data), table.size);

    if (table_meta == NULL)
        return make_error(env, erlcass::kUnknownTable);

    return enif_make_tuple2(env, ATOMS.atomOk, cass_table_meta_fields_to_erlang_term(env, table_meta));
}
