#include "nif_cass_uuid.h"
#include "erlcass.h"
#include "nif_utils.h"
#include "uuid_serialization.h"
#include "constants.h"
#include "macros.h"

ERL_NIF_TERM cass_uuid_to_nif(ErlNifEnv* env, const CassUuid& obj)
{
    char buffer[CASS_UUID_STRING_LENGTH];
    erlcass::cass_uuid_string(obj, buffer);

    return enif_make_tuple2(env, ATOMS.atomOk, make_binary(env, buffer, CASS_UUID_STRING_LENGTH - 1));
}

ERL_NIF_TERM nif_cass_uuid_gen_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    if(!data->uuid_gen)
        return make_error(env, erlcass::kInvalidUuidGeneratorMsg);

    CassUuid obj;
    cass_uuid_gen_time(data->uuid_gen, &obj);
    return cass_uuid_to_nif(env, obj);
}

ERL_NIF_TERM nif_cass_uuid_gen_random(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    if(!data->uuid_gen)
        return make_error(env, erlcass::kInvalidUuidGeneratorMsg);

    CassUuid obj;
    cass_uuid_gen_random(data->uuid_gen, &obj);
    return cass_uuid_to_nif(env, obj);
}

ERL_NIF_TERM nif_cass_uuid_gen_from_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    if(!data->uuid_gen)
        return make_error(env, erlcass::kInvalidUuidGeneratorMsg);

    unsigned long timestamp;

    if(!enif_get_uint64(env, argv[0], &timestamp))
        return make_badarg(env);

    CassUuid obj;
    cass_uuid_gen_from_time(data->uuid_gen, timestamp, &obj);
    return cass_uuid_to_nif(env, obj);
}

ERL_NIF_TERM nif_cass_uuid_min_from_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    unsigned long timestamp;

    if(!enif_get_uint64(env, argv[0], &timestamp))
        return make_badarg(env);

    CassUuid obj;
    cass_uuid_min_from_time(timestamp, &obj);
    return cass_uuid_to_nif(env, obj);
}

ERL_NIF_TERM nif_cass_uuid_max_from_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    unsigned long timestamp;

    if(!enif_get_uint64(env, argv[0], &timestamp))
        return make_badarg(env);

    CassUuid obj;
    cass_uuid_max_from_time(timestamp, &obj);
    return cass_uuid_to_nif(env, obj);
}

ERL_NIF_TERM nif_cass_uuid_timestamp(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;

    if(!get_bstring(env, argv[0], &bin))
        return make_badarg(env);

    CassUuid uuid;
    if(erlcass::cass_uuid_from_string_n(BIN_TO_STR(bin.data), bin.size, &uuid) != CASS_OK)
        return make_badarg(env);

    return enif_make_tuple2(env, ATOMS.atomOk, enif_make_uint64(env, cass_uuid_timestamp(uuid)));
}

ERL_NIF_TERM nif_cass_uuid_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;

    if(!get_bstring(env, argv[0], &bin))
        return make_badarg(env);

    CassUuid uuid;
    if(erlcass::cass_uuid_from_string_n(BIN_TO_STR(bin.data), bin.size, &uuid) != CASS_OK)
        return make_badarg(env);

    return enif_make_tuple2(env, ATOMS.atomOk, enif_make_int(env, cass_uuid_version(uuid)));
}
