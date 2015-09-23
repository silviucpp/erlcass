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

bool cass_tuple_set_from_nif(ErlNifEnv* env, CassTuple* tuple, int index, const SchemaColumn& type, ERL_NIF_TERM value, CassError* cass_error)
{
    switch (type.type)
    {
        case CASS_VALUE_TYPE_VARCHAR:
        case CASS_VALUE_TYPE_ASCII:
        case CASS_VALUE_TYPE_TEXT:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return false;
            
            *cass_error = cass_tuple_set_string_n(tuple, index, str_value.c_str(), str_value.length());
            break;
        }
            
        case CASS_VALUE_TYPE_INT:
        {
            int int_value = 0;
            
            if(!enif_get_int(env, value, &int_value ))
                return false;
            
            *cass_error = cass_tuple_set_int32(tuple, index, int_value);
            break;
        }
            
        case CASS_VALUE_TYPE_TIMESTAMP:
        case CASS_VALUE_TYPE_COUNTER:
        case CASS_VALUE_TYPE_BIGINT:
        {
            long long_value = 0;
            
            if(!enif_get_int64(env, value, &long_value ))
                return false;
            
            *cass_error = cass_tuple_set_int64(tuple, index, long_value);
            break;
        }
            
        case CASS_VALUE_TYPE_VARINT:
        case CASS_VALUE_TYPE_BLOB:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return false;
            
            *cass_error = cass_tuple_set_bytes(tuple, index, reinterpret_cast<const cass_byte_t*>(str_value.data()), str_value.size());
            break;
        }
            
        case CASS_VALUE_TYPE_BOOLEAN:
        {
            cass_bool_t bool_value = enif_is_identical(ATOMS.atomTrue, value) ? cass_true : cass_false;
            *cass_error = cass_tuple_set_bool(tuple, index, bool_value);
            break;
        }
            
        case CASS_VALUE_TYPE_FLOAT:
        case CASS_VALUE_TYPE_DOUBLE:
        {
            double val_double;
            if(!enif_get_double(env, value, &val_double))
                return false;
            
            if(type.type == CASS_VALUE_TYPE_FLOAT)
                *cass_error = cass_tuple_set_float(tuple, index, static_cast<float>(val_double));
            else
                *cass_error = cass_tuple_set_double(tuple, index, val_double);
            break;
        }
            
        case CASS_VALUE_TYPE_INET:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return false;
            
            CassInet inet;
            if(cass_inet_from_string_n(str_value.c_str(), str_value.length(), &inet) != CASS_OK)
                return false;
            
            *cass_error = cass_tuple_set_inet(tuple, index, inet);
            break;
        }
            
        case CASS_VALUE_TYPE_TIMEUUID:
        case CASS_VALUE_TYPE_UUID:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return false;
            
            CassUuid uuid;
            if(erlcass::cass_uuid_from_string_n(str_value.c_str(), str_value.length(), &uuid) != CASS_OK)
                return false;
            
            *cass_error = cass_tuple_set_uuid(tuple, index, uuid);
            break;
        }
            
        case CASS_VALUE_TYPE_DECIMAL:
        {
            const ERL_NIF_TERM *items;
            int arity;
            
            if(!enif_get_tuple(env, value, &arity, &items) || arity != 2)
                return false;
            
            std::string varint;
            int scale;
            
            if(!get_string(env, items[0], varint) || !enif_get_int(env, items[1], &scale))
                return false;
            
            *cass_error = cass_tuple_set_decimal(tuple, index, reinterpret_cast<const cass_byte_t*>(varint.data()), varint.size(), scale);
            break;
        }
            
        case CASS_VALUE_TYPE_MAP:
        case CASS_VALUE_TYPE_LIST:
        case CASS_VALUE_TYPE_SET:
        {
            CassCollection* nested_collection = nif_list_to_cass_collection(env, value, type, cass_error);
            
            if(!nested_collection)
                return false;
            
            if(*cass_error == CASS_OK)
                *cass_error = cass_tuple_set_collection(tuple, index, nested_collection);
            
            cass_collection_free(nested_collection);
            
            break;
        }
            
        case CASS_VALUE_TYPE_TUPLE:
        {
            CassTuple* nested_tuple = nif_term_to_cass_tuple(env, value, type, cass_error);
            
            if(!nested_tuple)
                return false;
            
            if(*cass_error == CASS_OK)
                *cass_error = cass_tuple_set_tuple(tuple, index, nested_tuple);
            
            cass_tuple_free(nested_tuple);
            
            break;
        }
            
        default:
            return false;
    }
    
    return true;
}

CassTuple* nif_term_to_cass_tuple(ErlNifEnv* env, ERL_NIF_TERM term, const SchemaColumn & type, CassError* error)
{
    const ERL_NIF_TERM *items;
    int arity;
    
    if(!enif_get_tuple(env, term, &arity, &items) || arity == 0 || static_cast<size_t>(arity) != type.subtypes.size())
        return NULL;
    
    CassTuple* tuple = cass_tuple_new(arity);
    
    for (int i = 0; i < arity; i++)
    {
        if(!cass_tuple_set_from_nif(env, tuple, i, type.subtypes.at(i), items[i], error))
        {
            cass_tuple_free(tuple);
            return NULL;
        }
        
        if(*error != CASS_OK)
            break;
    }
    
    return tuple;
}
