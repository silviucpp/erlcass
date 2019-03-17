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

ERL_NIF_TERM make_error(ErlNifEnv* env, ERL_NIF_TERM term)
{
    return enif_make_tuple2(env, ATOMS.atomError, term);
}

ERL_NIF_TERM make_error(ErlNifEnv* env, const char* error, size_t length)
{
    return make_error(env, make_binary(env, error, length));
}

ERL_NIF_TERM make_error(ErlNifEnv* env, const char* error)
{
    return make_error(env, error, strlen(error));
}

ERL_NIF_TERM make_badarg(ErlNifEnv* env)
{
    return enif_make_tuple2(env, ATOMS.atomError, ATOMS.atomBadArg);
}

ERL_NIF_TERM make_bad_options(ErlNifEnv* env, ERL_NIF_TERM term)
{
    return make_error(env, enif_make_tuple(env, 2, ATOMS.atomOptions, term));
}

bool get_bstring(ErlNifEnv* env, ERL_NIF_TERM term, ErlNifBinary* bin)
{
    if(enif_is_binary(env, term))
        return enif_inspect_binary(env, term, bin);

    return enif_inspect_iolist_as_binary(env, term, bin);
}

int get_atom(ErlNifEnv* env, ERL_NIF_TERM term, char* buf, size_t buffer_size)
{
    return enif_get_atom(env, term, buf, buffer_size, ERL_NIF_LATIN1);
}

bool get_boolean(ERL_NIF_TERM term, cass_bool_t* val)
{
    if(enif_is_identical(term, ATOMS.atomTrue))
    {
        *val = cass_true;
        return true;
    }

    if(enif_is_identical(term, ATOMS.atomFalse))
    {
        *val = cass_false;
        return true;
    }

    return false;
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

ERL_NIF_TERM parse_consistency_level_options(ErlNifEnv* env, ERL_NIF_TERM options_list, ConsistencyLevelOptions* cls)
{
    ERL_NIF_TERM head;
    const ERL_NIF_TERM *items;
    int arity;

    while(enif_get_list_cell(env, options_list, &head, &options_list))
    {
        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return make_bad_options(env, head);

        ERL_NIF_TERM key = items[0];
        ERL_NIF_TERM value = items[1];

        if(enif_is_identical(key, ATOMS.atomConsistencyLevel))
        {
            int c_level;

            if(!enif_get_int(env, value, &c_level))
                return make_bad_options(env, head);

            cls->cl = static_cast<CassConsistency>(c_level);
        }
        else if(enif_is_identical(key, ATOMS.atomSerialConsistencyLevel))
        {
            int c_level;

            if(!enif_get_int(env, value, &c_level))
                return make_bad_options(env, head);

            cls->serial_cl = static_cast<CassConsistency>(c_level);
        }
        else
        {
            return make_bad_options(env, head);
        }
    }

    return ATOMS.atomOk;
}

ERL_NIF_TERM parse_query_term(ErlNifEnv* env, ERL_NIF_TERM qterm, QueryTerm* q)
{
    if(enif_is_tuple(env, qterm))
    {
        const ERL_NIF_TERM *items;
        int arity;

        if(!enif_get_tuple(env, qterm, &arity, &items) || arity != 2)
            return make_badarg(env);

        if(!get_bstring(env, items[0], &q->query))
            return make_badarg(env);

        if(enif_is_list(env, items[1]))
        {
            return parse_consistency_level_options(env, items[1], &q->consistency);
        }
        else
        {
            int c_level;

            if(!enif_get_int(env, items[1], &c_level))
                return make_badarg(env);

            q->consistency.cl = static_cast<CassConsistency>(c_level);
        }
    }
    else
    {
        if(!get_bstring(env, qterm, &q->query))
            return make_badarg(env);
    }

    return ATOMS.atomOk;
}
