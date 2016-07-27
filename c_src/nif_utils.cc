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

bool parse_consistency_level_options(ErlNifEnv* env, ERL_NIF_TERM options_list, CassConsistency* cl, CassConsistency* serial_cl)
{
    ERL_NIF_TERM head;
    int c_level;

    while(enif_get_list_cell(env, options_list, &head, &options_list))
    {
        const ERL_NIF_TERM *items;
        int arity;

        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return false;

        if(enif_is_identical(items[0], ATOMS.atomConsistencyLevel))
        {
            if(!enif_get_int(env, items[1], &c_level))
                return false;

            *cl = static_cast<CassConsistency>(c_level);
        }
        else if(enif_is_identical(items[0], ATOMS.atomSerialConsistencyLevel))
        {
            if(!enif_get_int(env, items[1], &c_level))
                return enif_make_badarg(env);

            *serial_cl = static_cast<CassConsistency>(c_level);
        }
        else
        {
            return false;
        }
    }

    return true;
}

bool parse_query_term(ErlNifEnv* env, ERL_NIF_TERM qterm, ErlNifBinary* query, CassConsistency* cl, CassConsistency* serial_cl)
{
    if(enif_is_tuple(env, qterm))
    {
        const ERL_NIF_TERM *items;
        int arity;

        if(!enif_get_tuple(env, qterm, &arity, &items) || arity != 2)
            return false;

        if(!get_bstring(env, items[0], query))
            return false;

        if(enif_is_list(env, items[1]))
        {
            if(!parse_consistency_level_options(env, items[1], cl, serial_cl))
                return false;
        }
        else
        {
            int c_level;

            if(!enif_get_int(env, items[1], &c_level))
                return false;

            *cl = static_cast<CassConsistency>(c_level);
        }
    }
    else
    {
        if(!get_bstring(env, qterm, query))
            return false;
    }

    return true;
}


