#include "utils.h"
#include "erlcass.h"
#include "serialization.hpp"

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
            ret =  enif_get_string(env, term, &*(value.begin()), len+1, ERL_NIF_LATIN1);
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

//more performant methods for converting CassUuid into string representation.
//around 5 times faster. 

void uint8_to_strhex(uint8_t bin, char* result)
{
    static char hex_str[] = "0123456789abcdef";
    static int size_of_bin = sizeof(uint8_t);
    
    for (int i = 0; i < size_of_bin; i++)
    {
        result[i * 2 + 0] = hex_str[(bin >> 4) & 0x0F];
        result[i * 2 + 1] = hex_str[(bin) & 0x0F];
    }
}

void cass_uuid_to_string(CassUuid uuid, char* output)
{
    size_t pos = 0;
    char encoded[16];
    cass::encode_uuid(encoded, uuid);
    
    for (size_t i = 0; i < 16; ++i)
    {
        char buf[2];
        uint8_to_strhex(static_cast<uint8_t>(encoded[i]), buf);
        
        if (i == 4 || i == 6 || i == 8 || i == 10)
            output[pos++] = '-';

        output[pos++] = buf[0];
        output[pos++] = buf[1];
    }
    
    output[pos] = '\0';
}
