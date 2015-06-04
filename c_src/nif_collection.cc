//
//  collection.cpp
//  erlcass
//
//  Created by silviu on 5/28/15.
//
//

#include "nif_collection.h"
#include "utils.h"

bool cass_collection_append_from_nif(ErlNifEnv* env, CassCollection* collection, const ItemType& type, ERL_NIF_TERM value, CassError* cass_error)
{
    switch (type.type)
    {
        case CASS_VALUE_TYPE_TEXT:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return false;
            
            *cass_error = cass_collection_append_string(collection, str_value.c_str());
            break;
        }
            
        case CASS_VALUE_TYPE_INT:
        {
            int int_value = 0;
            
            if(!enif_get_int(env, value, &int_value ))
                return false;
            
            *cass_error = cass_collection_append_int32(collection, int_value);
            break;
        }
            
        case CASS_VALUE_TYPE_BIGINT:
        {
            long long_value = 0;
            
            if(!enif_get_int64(env, value, &long_value ))
                return false;
            
            *cass_error = cass_collection_append_int64(collection, long_value);
            break;
        }
            
        case CASS_VALUE_TYPE_BLOB:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return false;
            
            *cass_error = cass_collection_append_bytes(collection, (cass_byte_t*)str_value.data(), str_value.size());
            break;
        }
            
        case CASS_VALUE_TYPE_BOOLEAN:
        {
            cass_bool_t bool_value = enif_is_identical(ATOMS.atomTrue, value) ? cass_true : cass_false;
            *cass_error = cass_collection_append_bool(collection, bool_value);
            break;
        }
            
        case CASS_VALUE_TYPE_FLOAT:
        case CASS_VALUE_TYPE_DOUBLE:
        {
            double val_double;
            if(!enif_get_double(env, value, &val_double))
                return false;
            
            if(type.type == CASS_VALUE_TYPE_FLOAT)
                *cass_error = cass_collection_append_float(collection, static_cast<float>(val_double));
            else
                *cass_error = cass_collection_append_double(collection, val_double);
            break;
        }
            
        case CASS_VALUE_TYPE_INET:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return false;
            
            CassInet inet;
            if(cass_inet_from_string(str_value.c_str(), &inet) != CASS_OK)
                return false;
            
            *cass_error = cass_collection_append_inet(collection, inet);
            break;
        }
            
        case CASS_VALUE_TYPE_TIMEUUID:
        case CASS_VALUE_TYPE_UUID:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return false;
            
            CassUuid uuid;
            if(cass_uuid_from_string(str_value.c_str(), &uuid) != CASS_OK)
                return false;
            
            *cass_error = cass_collection_append_uuid(collection, uuid);
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
            
            *cass_error = cass_collection_append_decimal(collection, (cass_byte_t*)varint.data(), varint.size(), scale);
            break;
        }
            
        default:
            return false;
    }
    
    return true;
}

CassCollectionType value_type_to_collection_type(CassValueType type)
{
    if(type == CASS_VALUE_TYPE_LIST)
        return CASS_COLLECTION_TYPE_LIST;
    else if(type == CASS_VALUE_TYPE_SET)
        return CASS_COLLECTION_TYPE_SET;
    
    return CASS_COLLECTION_TYPE_MAP;
}

bool populate_list_set_collection(ErlNifEnv* env, ERL_NIF_TERM list, CassCollection *collection, const ItemType &type, CassError* error)
{
    ERL_NIF_TERM head;
    
    while(enif_get_list_cell(env, list, &head, &list))
    {
        if(!cass_collection_append_from_nif(env, collection, type.valueType, head, error))
            return false;
        
        if(*error != CASS_OK)
            break;
    }
    
    return error;
}

bool populate_map_collection(ErlNifEnv* env, ERL_NIF_TERM list, CassCollection *collection, const ItemType& type, CassError* error)
{
    ERL_NIF_TERM head;
    const ERL_NIF_TERM *items;
    int arity;
    
    while(enif_get_list_cell(env, list, &head, &list))
    {
        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return false;
        
        //add key
        
        if(!cass_collection_append_from_nif(env, collection, type.keyType, items[0], error))
            break;
        
        if(*error != CASS_OK)
            break;
        
        //add value
        
        if(!cass_collection_append_from_nif(env, collection, type.valueType, items[1], error))
            return false;
        
        if(*error != CASS_OK)
            break;
    }
    
    return true;
}

CassCollection* nif_list_to_cass_collection(ErlNifEnv* env, ERL_NIF_TERM list, const ItemType & type, CassError * error)
{
    unsigned int length;
    
    if(!enif_get_list_length(env, list, &length))
        return NULL;
    
    CassCollection* collection = cass_collection_new(value_type_to_collection_type(type.type), length);
    
    bool success;
    
    if(type.type != CASS_VALUE_TYPE_MAP)
        success = populate_list_set_collection(env, list, collection, type, error);
    else
        success = populate_map_collection(env, list, collection, type, error);

    if(!success)
    {
        cass_collection_free(collection);
        collection = NULL;
    }
    
    return collection;
}

