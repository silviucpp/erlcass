#include "nif_cass_prepared.h"
#include "nif_cass_statement.h"
#include "nif_utils.h"
#include "constants.h"

struct enif_cass_prepared
{
    const CassPrepared* prepared;
    ConsistencyLevelOptions consistency;
};

ERL_NIF_TERM nif_cass_prepared_new(ErlNifEnv* env, ErlNifResourceType* rs, const CassPrepared* prep, const ConsistencyLevelOptions& consistency)
{
    enif_cass_prepared *enif_obj = static_cast<enif_cass_prepared*>(enif_alloc_resource(rs, sizeof(enif_cass_prepared)));

    if(enif_obj == NULL)
        return make_error(env, erlcass::kFailedToAllocResourceMsg);

    enif_obj->prepared = prep;
    enif_obj->consistency = consistency;

    ERL_NIF_TERM term = enif_make_resource(env, enif_obj);
    enif_release_resource(enif_obj);

    return term;
}

void nif_cass_prepared_free(ErlNifEnv* env, void* obj)
{
    enif_cass_prepared *enif_prepared = static_cast<enif_cass_prepared*>(obj);

    if(enif_prepared->prepared != NULL)
        cass_prepared_free(enif_prepared->prepared);
}

ERL_NIF_TERM nif_cass_prepared_bind(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    enif_cass_prepared * enif_prep = NULL;

    if(!enif_get_resource(env, argv[0], data->resCassPrepared, reinterpret_cast<void**>(&enif_prep)))
        return make_badarg(env);

    ERL_NIF_TERM term = nif_cass_statement_new(env, data->resCassStatement, enif_prep->prepared, enif_prep->consistency);

    if(enif_is_tuple(env, term))
        return term;

    return enif_make_tuple2(env, ATOMS.atomOk, term);
}

