//
//  collection.cpp
//  erlcass
//
//  Created by silviu on 5/28/15.
//
//

#include "nif_collection.h"
#include "nif_utils.h"
#include "nif_tuple.h"
#include "erlcass.h"
#include "uuid_serialization.h"
#include "constants.h"

ERL_NIF_TERM cass_collection_append_from_nif(ErlNifEnv* env, CassCollection* collection, const SchemaColumn& type, ERL_NIF_TERM value)
{
    switch (type.type)
    {
        case CASS_VALUE_TYPE_VARCHAR:
        case CASS_VALUE_TYPE_ASCII:
        case CASS_VALUE_TYPE_TEXT:
        {
            ErlNifBinary bin;
            
            if(!get_bstring(env, value, &bin))
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_collection_append_string_n(collection, BIN_TO_STR(bin.data), bin.size));
        }
            
        case CASS_VALUE_TYPE_TINY_INT:
        {
            int int_value = 0;
            
            if(!enif_get_int(env, value, &int_value ))
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_collection_append_int8(collection, static_cast<cass_int8_t>(int_value)));
        }
            
        case CASS_VALUE_TYPE_SMALL_INT:
        {
            int int_value = 0;
            
            if(!enif_get_int(env, value, &int_value ))
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_collection_append_int16(collection, static_cast<cass_int16_t>(int_value)));
        }

        case CASS_VALUE_TYPE_INT:
        {
            int int_value = 0;
            
            if(!enif_get_int(env, value, &int_value ))
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_collection_append_int32(collection, int_value));
        }
            
        case CASS_VALUE_TYPE_DATE:
        {
            unsigned int uint_value = 0;
            
            if(!enif_get_uint(env, value, &uint_value ))
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_collection_append_uint32(collection, uint_value));
        }

        case CASS_VALUE_TYPE_TIME:
        case CASS_VALUE_TYPE_TIMESTAMP:
        case CASS_VALUE_TYPE_COUNTER:
        case CASS_VALUE_TYPE_BIGINT:
        {
            long long_value = 0;
            
            if(!enif_get_int64(env, value, &long_value ))
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_collection_append_int64(collection, long_value));
        }
            
        case CASS_VALUE_TYPE_VARINT:            
        case CASS_VALUE_TYPE_BLOB:
        {
            ErlNifBinary bin;
            
            if(!get_bstring(env, value, &bin))
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_collection_append_bytes(collection, bin.data, bin.size));
        }
            
        case CASS_VALUE_TYPE_BOOLEAN:
        {
            cass_bool_t bool_value = static_cast<cass_bool_t>(enif_is_identical(ATOMS.atomTrue, value));
            return cass_error_to_nif_term(env, cass_collection_append_bool(collection, bool_value));
        }
            
        case CASS_VALUE_TYPE_FLOAT:
        case CASS_VALUE_TYPE_DOUBLE:
        {
            double val_double;
            if(!enif_get_double(env, value, &val_double))
                return enif_make_badarg(env);
            
            if(type.type == CASS_VALUE_TYPE_FLOAT)
                return cass_error_to_nif_term(env, cass_collection_append_float(collection, static_cast<float>(val_double)));
            else
                return cass_error_to_nif_term(env, cass_collection_append_double(collection, val_double));
        }
            
        case CASS_VALUE_TYPE_INET:
        {
            ErlNifBinary bin;
            
            if(!get_bstring(env, value, &bin))
                return enif_make_badarg(env);
            
            CassInet inet;
            if(cass_inet_from_string_n(BIN_TO_STR(bin.data), bin.size, &inet) != CASS_OK)
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_collection_append_inet(collection, inet));
        }
            
        case CASS_VALUE_TYPE_TIMEUUID:
        case CASS_VALUE_TYPE_UUID:
        {
            ErlNifBinary bin;
            
            if(!get_bstring(env, value, &bin))
                return enif_make_badarg(env);
            
            CassUuid uuid;
            if(erlcass::cass_uuid_from_string_n(BIN_TO_STR(bin.data), bin.size, &uuid) != CASS_OK)
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_collection_append_uuid(collection, uuid));
        }
            
        case CASS_VALUE_TYPE_DECIMAL:
        {
            const ERL_NIF_TERM *items;
            int arity;
            
            if(!enif_get_tuple(env, value, &arity, &items) || arity != 2)
                return enif_make_badarg(env);
            
            ErlNifBinary varint;
            int scale;
            
            if(!get_bstring(env, items[0], &varint) || !enif_get_int(env, items[1], &scale))
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_collection_append_decimal(collection, varint.data, varint.size, scale));
        }
            
        case CASS_VALUE_TYPE_MAP:
        case CASS_VALUE_TYPE_LIST:
        case CASS_VALUE_TYPE_SET:
        {
            CassCollection* nested_collection = NULL;
            
            ERL_NIF_TERM result = nif_list_to_cass_collection(env, value, type, &nested_collection);
            
            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;
            
            CassError error = cass_collection_append_collection(collection, nested_collection);
            cass_collection_free(nested_collection);
            return cass_error_to_nif_term(env, error);
        }
            
        case CASS_VALUE_TYPE_TUPLE:
        {
            CassTuple* tuple = NULL;
            
            ERL_NIF_TERM result = nif_term_to_cass_tuple(env, value, type, &tuple);
            
            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;
            
            CassError error = cass_collection_append_tuple(collection, tuple);
            cass_tuple_free(tuple);
            return cass_error_to_nif_term(env, error);
        }
            
        //not implemented types
        default:
            return make_error(env, erlcass::kFailedToAddUnknownTypeInCollection);
    }
}

ERL_NIF_TERM populate_list_set_collection(ErlNifEnv* env, ERL_NIF_TERM list, CassCollection *collection, const SchemaColumn &type)
{
    ERL_NIF_TERM head;
    ERL_NIF_TERM item_term;
    
    while(enif_get_list_cell(env, list, &head, &list))
    {
        item_term = cass_collection_append_from_nif(env, collection, type.subtypes[KEY_INDEX], head);
        
        if(!enif_is_identical(item_term, ATOMS.atomOk))
            return item_term;
    }
    
    return ATOMS.atomOk;
}

ERL_NIF_TERM populate_map_collection(ErlNifEnv* env, ERL_NIF_TERM list, CassCollection *collection, const SchemaColumn& type)
{
    ERL_NIF_TERM head;
    ERL_NIF_TERM item_term;
    const ERL_NIF_TERM *items;
    int arity;
    
    while(enif_get_list_cell(env, list, &head, &list))
    {
        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return enif_make_badarg(env);
        
        //add key
        item_term = cass_collection_append_from_nif(env, collection, type.subtypes[KEY_INDEX], items[0]);
        
        if(!enif_is_identical(item_term, ATOMS.atomOk))
            return item_term;
        
        //add value
        
        item_term = cass_collection_append_from_nif(env, collection, type.subtypes[VAL_INDEX], items[1]);
        
        if(!enif_is_identical(item_term, ATOMS.atomOk))
            return item_term;
    }
    
    return ATOMS.atomOk;
}

ERL_NIF_TERM nif_list_to_cass_collection(ErlNifEnv* env, ERL_NIF_TERM list, const  SchemaColumn& type, CassCollection ** col)
{
    unsigned int length;
    
    if(!enif_get_list_length(env, list, &length))
        return enif_make_badarg(env);
    
    CassCollection* collection = cass_collection_new(static_cast<CassCollectionType>(type.type), length);
    
    ERL_NIF_TERM return_value;
    
    if(type.type != CASS_VALUE_TYPE_MAP)
        return_value = populate_list_set_collection(env, list, collection, type);
    else
        return_value = populate_map_collection(env, list, collection, type);

    if(!enif_is_identical(return_value, ATOMS.atomOk))
        cass_collection_free(collection);
    else
        *col = collection;
    
    return return_value;
}
