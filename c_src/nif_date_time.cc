#include "nif_date_time.h"
#include "cassandra.h"
#include "nif_utils.h"

ERL_NIF_TERM nif_cass_date_from_epoch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long epoch_secs;

    if(!enif_get_int64(env, argv[0], &epoch_secs))
        return make_badarg(env);

    return enif_make_uint(env, cass_date_from_epoch(epoch_secs));
}

ERL_NIF_TERM nif_cass_time_from_epoch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    long epoch_secs;

    if(!enif_get_int64(env, argv[0], &epoch_secs))
        return make_badarg(env);

    return enif_make_int64(env, cass_time_from_epoch(epoch_secs));
}

ERL_NIF_TERM nif_cass_date_time_to_epoch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    unsigned int date;
    long time;

    if(!enif_get_uint(env, argv[0], &date) || !enif_get_int64(env, argv[1], &time))
        return make_badarg(env);

    return enif_make_int64(env, cass_date_time_to_epoch(date, time));
}
