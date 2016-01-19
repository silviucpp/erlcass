#include "nif_utils.h"
#include "erlcass.h"

#include <string.h>

ERL_NIF_TERM make_atom(ErlNifEnv* env, const char* name)
{
    ERL_NIF_TERM ret;
    
    if(enif_make_existing_atom(env, name, &ret, ERL_NIF_LATIN1))
        return ret;

    return enif_make_atom(env, name);
}

ERL_NIF_TERM make_binary(ErlNifEnv* env, const char* buff, size_t length)
{
    ERL_NIF_TERM term;
    unsigned char *destination_buffer = enif_make_new_binary(env, length, &term);
    memcpy(destination_buffer, buff, length);
    return term;
}

ERL_NIF_TERM make_error(ErlNifEnv* env, const char* error, size_t length)
{
    return enif_make_tuple2(env, ATOMS.atomError, make_binary(env, error, length));
}

ERL_NIF_TERM make_error(ErlNifEnv* env, const char* error)
{
    return make_error(env, error, strlen(error));
}

bool get_bstring(ErlNifEnv* env, ERL_NIF_TERM term, ErlNifBinary* bin)
{
    if(enif_is_binary(env, term))
        return enif_inspect_binary(env, term, bin);

    return enif_inspect_iolist_as_binary(env, term, bin);
}

ERL_NIF_TERM cass_error_to_nif_term(ErlNifEnv* env, CassError error)
{
    if(error != CASS_OK)
        return make_error(env, cass_error_desc(error));
    
    return ATOMS.atomOk;
}

ERL_NIF_TERM cass_future_error_to_nif_term(ErlNifEnv* env, CassFuture* future)
{
    const char* message;
    size_t message_length;
    cass_future_error_message(future, &message, &message_length);
    return make_error(env, message, message_length);
}
