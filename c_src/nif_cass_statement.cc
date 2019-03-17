#include "nif_cass_statement.h"
#include "cass_binding.h"
#include "execute_request.hpp"
#include "constants.h"
#include "macros.h"

#define BIND_BY_INDEX 1
#define BIND_BY_NAME  2

struct enif_cass_statement
{
    CassStatement* statement;
};

ERL_NIF_TERM bind_prepared_statement_params(ErlNifEnv* env, CassStatement* statement, int type, ERL_NIF_TERM list)
{
    ERL_NIF_TERM head;

    datastax::internal::core::Statement* stm = static_cast<datastax::internal::core::Statement*>(statement);
    const datastax::internal::core::ResultResponse* result = static_cast<datastax::internal::core::ExecuteRequest*>(stm)->prepared()->result().get();

    if(type == BIND_BY_NAME)
    {
        // bind by name -> {name, value}

        datastax::internal::core::IndexVec indices;
        ErlNifBinary column_name;
        const ERL_NIF_TERM *items;
        int arity;

        while(enif_get_list_cell(env, list, &head, &list))
        {
            if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
                return make_badarg(env);

            if(!get_bstring(env, items[0], &column_name))
                return make_badarg(env);

            if(result->metadata()->get_indices(datastax::StringRef(BIN_TO_STR(column_name.data), column_name.size), &indices) == 0)
                return make_badarg(env);

            size_t index = indices[0];

            const datastax::internal::core::DataType* data_type = result->metadata()->get_column_definition(index).data_type.get();
            ERL_NIF_TERM nif_result = cass_bind_by_index(env, statement, index, data_type, items[1]);

            if(!enif_is_identical(nif_result, ATOMS.atomOk))
                return nif_result;
        }
    }
    else
    {
        // bind by index

        size_t index = 0;

        while(enif_get_list_cell(env, list, &head, &list))
        {
            if(index > result->metadata()->column_count())
                return make_badarg(env);

            const datastax::internal::core::ColumnDefinition def = result->metadata()->get_column_definition(index);

            if(def.data_type.get() == NULL)
                return make_badarg(env);

            const datastax::internal::core::DataType* data_type = def.data_type.get();

            ERL_NIF_TERM nif_result = cass_bind_by_index(env, statement, index, data_type, head);

            if(!enif_is_identical(nif_result, ATOMS.atomOk))
                return nif_result;

            index++;
        }
    }

    return ATOMS.atomOk;
}

CassStatement* get_statement(ErlNifEnv* env, ErlNifResourceType* resource_type, ERL_NIF_TERM arg)
{
    enif_cass_statement * enif_stm = NULL;

    if(!enif_get_resource(env, arg, resource_type, reinterpret_cast<void**>(&enif_stm)))
        return NULL;

    return enif_stm->statement;
}

ERL_NIF_TERM nif_cass_statement_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    QueryTerm q(ConsistencyLevelOptions(data->defaultConsistencyLevel, CASS_CONSISTENCY_ANY));

    ERL_NIF_TERM parse_result = parse_query_term(env, argv[0], &q);

    if(!enif_is_identical(ATOMS.atomOk, parse_result))
        return parse_result;

    CassStatement* stm = cass_statement_new_n(BIN_TO_STR(q.query.data), q.query.size, 0);

    CassError cass_result = cass_statement_set_consistency(stm, q.consistency.cl);

    if(cass_result != CASS_OK)
        return cass_error_to_nif_term(env, cass_result);

    if(q.consistency.serial_cl != CASS_CONSISTENCY_ANY)
    {
        cass_result = cass_statement_set_serial_consistency(stm, q.consistency.serial_cl);

        if(cass_result != CASS_OK)
            return cass_error_to_nif_term(env, cass_result);
    }

    enif_cass_statement *enif_obj = static_cast<enif_cass_statement*>(enif_alloc_resource(data->resCassStatement, sizeof(enif_cass_statement)));

    if(enif_obj == NULL)
        return make_error(env, erlcass::kFailedToAllocResourceMsg);

    enif_obj->statement = stm;

    ERL_NIF_TERM term = enif_make_resource(env, enif_obj);
    enif_release_resource(enif_obj);

    return enif_make_tuple2(env, ATOMS.atomOk, term);
}

ERL_NIF_TERM nif_cass_statement_new(ErlNifEnv* env, ErlNifResourceType* resource_type, const CassPrepared* prep, const ConsistencyLevelOptions& consistency)
{
    enif_cass_statement *enif_obj = static_cast<enif_cass_statement*>(enif_alloc_resource(resource_type, sizeof(enif_cass_statement)));

    if(enif_obj == NULL)
        return make_error(env, erlcass::kFailedToAllocResourceMsg);

    enif_obj->statement = cass_prepared_bind(prep);

    CassError cass_result = cass_statement_set_consistency(enif_obj->statement, consistency.cl);

    if(cass_result != CASS_OK)
        return cass_error_to_nif_term(env, cass_result);

    if(consistency.serial_cl != CASS_CONSISTENCY_ANY )
    {
        cass_result = cass_statement_set_serial_consistency(enif_obj->statement, consistency.serial_cl);

        if(cass_result != CASS_OK)
            return cass_error_to_nif_term(env, cass_result);
    }

    ERL_NIF_TERM term = enif_make_resource(env, enif_obj);
    enif_release_resource(enif_obj);

    return term;
}

void nif_cass_statement_free(ErlNifEnv* env, void* obj)
{
    enif_cass_statement *data = static_cast<enif_cass_statement*>(obj);

    if(data->statement != NULL)
        cass_statement_free(data->statement);
}

ERL_NIF_TERM nif_cass_statement_bind_parameters(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    enif_cass_statement * enif_stm = NULL;

    if(!enif_get_resource(env, argv[0], data->resCassStatement, reinterpret_cast<void**>(&enif_stm)) || !enif_is_list(env, argv[2]))
        return make_badarg(env);

    int bind_type;

    if(!enif_get_int(env, argv[1], &bind_type) || (bind_type != BIND_BY_INDEX && bind_type != BIND_BY_NAME))
        return make_badarg(env);

    return bind_prepared_statement_params(env, enif_stm->statement, bind_type, argv[2]);
}

ERL_NIF_TERM nif_cass_statement_set_paging_size(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    enif_cass_statement * enif_stm = NULL;

    if(!enif_get_resource(env, argv[0], data->resCassStatement, (void**) &enif_stm))
        return make_badarg(env);

    int page_size = 0;

    if((!enif_get_int(env, argv[1], &page_size)) || (page_size < 1))
        return make_badarg(env);

    cass_statement_set_paging_size(enif_stm->statement, page_size);

    return ATOMS.atomOk;
}
