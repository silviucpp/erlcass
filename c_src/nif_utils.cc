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

ERL_NIF_TERM make_error(ErlNifEnv* env, const char* error)
{
    return enif_make_tuple2(env, ATOMS.atomError, make_binary(env, error, strlen(error)));
}

bool get_atom(ErlNifEnv* env, ERL_NIF_TERM term, std::string & value)
{
    unsigned len;
    
    if(!enif_get_atom_length(env, term, &len, ERL_NIF_LATIN1))
        return false;
    
    value.resize(len+1);
    return enif_get_atom(env, term, &*(value.begin()), len+1, ERL_NIF_LATIN1);
}

bool get_string(ErlNifEnv* env, ERL_NIF_TERM term, std::string & value)
{
    if(enif_is_binary(env, term))
    {
        ErlNifBinary bin;
        
        if(enif_inspect_binary(env, term, &bin))
        {
            value.resize(bin.size);
            memcpy((char*)value.c_str(), bin.data, bin.size);
            return true;
        }
    }
    else
    {
        unsigned len;
        bool ret = enif_get_list_length(env, term, &len);
        
        if(ret)
        {
            value.resize(len+1);
            ret =  enif_get_string(env, term, &*(value.begin()), value.size(), ERL_NIF_LATIN1);
            
            if(ret > 0)
                value.resize(len); //trim null terminated char.
        }
        
        return ret;
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
    
    std::string msg(message, message_length);
    return make_error(env, msg.c_str());
}
