//
//  nif_tuple.cpp
//  erlcass
//
//  Created by silviu on 9/23/15.
//
//

#include "nif_tuple.h"
#include "nif_utils.h"
#include "nif_collection.h"
#include "erlcass.h"
#include "uuid_serialization.h"
#include "cassandra.h"

ERL_NIF_TERM cass_tuple_set_from_nif(ErlNifEnv* env, CassTuple* tuple, int index, const SchemaColumn& type, ERL_NIF_TERM value)
{
    switch (type.type)
    {
        case CASS_VALUE_TYPE_VARCHAR:
        case CASS_VALUE_TYPE_ASCII:
        case CASS_VALUE_TYPE_TEXT:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_tuple_set_string_n(tuple, index, str_value.c_str(), str_value.length()));
        }
            
        case CASS_VALUE_TYPE_INT:
        {
            int int_value = 0;
            
            if(!enif_get_int(env, value, &int_value ))
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_tuple_set_int32(tuple, index, int_value));
        }
            
        case CASS_VALUE_TYPE_TIMESTAMP:
        case CASS_VALUE_TYPE_COUNTER:
        case CASS_VALUE_TYPE_BIGINT:
        {
            long long_value = 0;
            
            if(!enif_get_int64(env, value, &long_value ))
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_tuple_set_int64(tuple, index, long_value));
        }
            
        case CASS_VALUE_TYPE_VARINT:
        case CASS_VALUE_TYPE_BLOB:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return enif_make_badarg(env);
            
            const cass_byte_t* bytes = reinterpret_cast<const cass_byte_t*>(str_value.data());
            return cass_error_to_nif_term(env, cass_tuple_set_bytes(tuple, index, bytes, str_value.size()));
        }
            
        case CASS_VALUE_TYPE_BOOLEAN:
        {
            cass_bool_t bool_value = static_cast<cass_bool_t>(enif_is_identical(ATOMS.atomTrue, value));
            return cass_error_to_nif_term(env, cass_tuple_set_bool(tuple, index, bool_value));
        }
            
        case CASS_VALUE_TYPE_FLOAT:
        case CASS_VALUE_TYPE_DOUBLE:
        {
            double val_double;
            if(!enif_get_double(env, value, &val_double))
                return enif_make_badarg(env);
            
            if(type.type == CASS_VALUE_TYPE_FLOAT)
                return cass_error_to_nif_term(env, cass_tuple_set_float(tuple, index, static_cast<float>(val_double)));
            else
                return cass_error_to_nif_term(env, cass_tuple_set_double(tuple, index, val_double));
        }
            
        case CASS_VALUE_TYPE_INET:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return enif_make_badarg(env);
            
            CassInet inet;
            if(cass_inet_from_string_n(str_value.c_str(), str_value.length(), &inet) != CASS_OK)
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_tuple_set_inet(tuple, index, inet));
        }
            
        case CASS_VALUE_TYPE_TIMEUUID:
        case CASS_VALUE_TYPE_UUID:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return enif_make_badarg(env);
            
            CassUuid uuid;
            if(erlcass::cass_uuid_from_string_n(str_value.c_str(), str_value.length(), &uuid) != CASS_OK)
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_tuple_set_uuid(tuple, index, uuid));
        }
            
        case CASS_VALUE_TYPE_DECIMAL:
        {
            const ERL_NIF_TERM *items;
            int arity;
            
            if(!enif_get_tuple(env, value, &arity, &items) || arity != 2)
                return enif_make_badarg(env);
            
            std::string varint;
            int scale;
            
            if(!get_string(env, items[0], varint) || !enif_get_int(env, items[1], &scale))
                return enif_make_badarg(env);
            
            const cass_byte_t* varint_bytes = reinterpret_cast<const cass_byte_t*>(varint.data());
            return cass_error_to_nif_term(env, cass_tuple_set_decimal(tuple, index, varint_bytes, varint.size(), scale));
        }
            
        case CASS_VALUE_TYPE_MAP:
        case CASS_VALUE_TYPE_LIST:
        case CASS_VALUE_TYPE_SET:
        {
            CassCollection* collection = NULL;
            
            ERL_NIF_TERM result = nif_list_to_cass_collection(env, value, type, &collection);
            
            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;
            
            CassError error = cass_tuple_set_collection(tuple, index, collection);
            cass_collection_free(collection);
            return cass_error_to_nif_term(env, error);
        }
            
        case CASS_VALUE_TYPE_TUPLE:
        {
            CassTuple* nested_tuple = NULL;
            
            ERL_NIF_TERM result = nif_term_to_cass_tuple(env, value, type, &nested_tuple);
            
            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;
            
            CassError error = cass_tuple_set_tuple(tuple, index, nested_tuple);
            cass_tuple_free(nested_tuple);
            return cass_error_to_nif_term(env, error);
        }
            
        default:
            return make_error(env, "failed to set unknown type into the tuple");
    }
}

ERL_NIF_TERM nif_term_to_cass_tuple(ErlNifEnv* env, ERL_NIF_TERM term, const SchemaColumn & type, CassTuple** tp)
{
    const ERL_NIF_TERM *items;
    int arity;
    
    if(!enif_get_tuple(env, term, &arity, &items) || arity == 0 || static_cast<size_t>(arity) != type.subtypes.size())
        return enif_make_badarg(env);
    
    CassTuple* tuple = cass_tuple_new(arity);
    ERL_NIF_TERM item_term;
    
    for (int i = 0; i < arity; i++)
    {
        item_term = cass_tuple_set_from_nif(env, tuple, i, type.subtypes.at(i), items[i]);
        
        if(!enif_is_identical(item_term, ATOMS.atomOk))
        {
            cass_tuple_free(tuple);
            return item_term;
        }
    }
    
    *tp = tuple;
    return ATOMS.atomOk;
}
