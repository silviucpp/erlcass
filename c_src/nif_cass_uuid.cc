//
//  nif_cass_uuid.cpp
//  erlcass
//
//  Created by silviu on 6/2/15.
//
//

#include "nif_cass_uuid.h"
#include "erlcass.h"
#include "nif_utils.h"
#include "uuid_serialization.h"

struct enif_cass_uuid_gen
{
    CassUuidGen* gen;
};

ERL_NIF_TERM nif_cass_uuid_gen_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    
    enif_cass_uuid_gen *enif_gen = static_cast<enif_cass_uuid_gen*>(enif_alloc_resource(data->resCassUuidGen, sizeof(enif_cass_uuid_gen)));
    
    if(enif_gen == NULL)
        return make_error(env, "enif_alloc_resource failed");
    
    enif_gen->gen = cass_uuid_gen_new();
    
    ERL_NIF_TERM term = enif_make_resource(env, enif_gen);
    enif_release_resource(enif_gen);
    
    return enif_make_tuple2(env, ATOMS.atomOk, term);
}

void nif_cass_uuid_gen_free(ErlNifEnv* env, void* obj)
{
    enif_cass_uuid_gen *enif_gen = static_cast<enif_cass_uuid_gen*>(obj);
    
    if(enif_gen->gen != NULL)
        cass_uuid_gen_free(enif_gen->gen);
}

ERL_NIF_TERM cass_uuid_to_nif(ErlNifEnv* env, const CassUuid& obj)
{
    char buffer[CASS_UUID_STRING_LENGTH];
    erlcass::cass_uuid_string(obj, buffer);
    
    return enif_make_tuple2(env, ATOMS.atomOk, make_binary(env, buffer, CASS_UUID_STRING_LENGTH - 1));
}

ERL_NIF_TERM nif_cass_uuid_gen_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    
    enif_cass_uuid_gen * enif_gen = NULL;
    
    if(!enif_get_resource(env, argv[0], data->resCassUuidGen, (void**) &enif_gen))
        return enif_make_badarg(env);
    
    CassUuid obj;
    cass_uuid_gen_time(enif_gen->gen, &obj);
    return cass_uuid_to_nif(env, obj);
}

ERL_NIF_TERM nif_cass_uuid_gen_random(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    
    enif_cass_uuid_gen * enif_gen = NULL;
    
    if(!enif_get_resource(env, argv[0], data->resCassUuidGen, (void**) &enif_gen))
        return enif_make_badarg(env);
    
    CassUuid obj;
    cass_uuid_gen_random(enif_gen->gen, &obj);
    return cass_uuid_to_nif(env, obj);
}

ERL_NIF_TERM nif_cass_uuid_gen_from_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    
    enif_cass_uuid_gen * enif_gen = NULL;
    
    if(!enif_get_resource(env, argv[0], data->resCassUuidGen, (void**) &enif_gen))
        return enif_make_badarg(env);
    
    unsigned long timestamp;
    
    if(!enif_get_uint64(env, argv[1], &timestamp))
        return enif_make_badarg(env);
    
    CassUuid obj;
    cass_uuid_gen_from_time(enif_gen->gen, timestamp, &obj);
    return cass_uuid_to_nif(env, obj);
}

ERL_NIF_TERM nif_cass_uuid_min_from_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    unsigned long timestamp;
    
    if(!enif_get_uint64(env, argv[0], &timestamp))
        return enif_make_badarg(env);
    
    CassUuid obj;
    cass_uuid_min_from_time(timestamp, &obj);
    return cass_uuid_to_nif(env, obj);
}

ERL_NIF_TERM nif_cass_uuid_max_from_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    unsigned long timestamp;
    
    if(!enif_get_uint64(env, argv[0], &timestamp))
        return enif_make_badarg(env);
    
    CassUuid obj;
    cass_uuid_max_from_time(timestamp, &obj);
    return cass_uuid_to_nif(env, obj);
}

ERL_NIF_TERM nif_cass_uuid_timestamp(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    std::string str_value;
    
    if(!get_string(env, argv[0], str_value))
        return enif_make_badarg(env);
    
    CassUuid uuid;
    if(erlcass::cass_uuid_from_string_n(str_value.c_str(), str_value.length(), &uuid) != CASS_OK)
        return enif_make_badarg(env);
    
    return enif_make_tuple2(env, ATOMS.atomOk, enif_make_uint64(env, cass_uuid_timestamp(uuid)));
}

ERL_NIF_TERM nif_cass_uuid_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    std::string str_value;
    
    if(!get_string(env, argv[0], str_value))
        return enif_make_badarg(env);
    
    CassUuid uuid;
    if(erlcass::cass_uuid_from_string_n(str_value.c_str(), str_value.length(), &uuid) != CASS_OK)
        return enif_make_badarg(env);
    
    return enif_make_tuple2(env, ATOMS.atomOk, enif_make_int(env,cass_uuid_version(uuid)));
}





