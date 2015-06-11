//
//  nif_cass_statement.cpp
//  erlcass
//
//  Created by silviu on 5/12/15.
//
//

#include "nif_cass_statement.h"
#include "nif_utils.h"
#include "metadata.h"
#include "nif_collection.h"
#include "uuid_serialization.h"

typedef struct
{
    CassStatement* statement;
    const ColumnsMap* columns_map;
}
enif_cass_statement;

bool bind_params_by_index(ErlNifEnv* env, CassStatement* statement, size_t index, const SchemaColumn& type, ERL_NIF_TERM value, CassError& cass_error)
{
    if(enif_is_identical(value, ATOMS.atomNull))
    {
        cass_error = cass_statement_bind_null(statement, index);
        return true;
    }
    
    switch (type.type)
    {
            
        case CASS_VALUE_TYPE_VARCHAR:
        case CASS_VALUE_TYPE_ASCII:
        case CASS_VALUE_TYPE_TEXT:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return false;
            
            cass_error = cass_statement_bind_string_n(statement, index, str_value.c_str(), str_value.length());
            break;
        }
            
        case CASS_VALUE_TYPE_INT:
        {
            int int_value = 0;
            
            if(!enif_get_int(env, value, &int_value ))
                return false;
            
            cass_error = cass_statement_bind_int32(statement, index, int_value);
            break;
        }
            
        case CASS_VALUE_TYPE_TIMESTAMP:
        case CASS_VALUE_TYPE_COUNTER:
        case CASS_VALUE_TYPE_BIGINT:
        {
            long long_value = 0;
            
            if(!enif_get_int64(env, value, &long_value ))
                return false;
            
            cass_error = cass_statement_bind_int64(statement, index, long_value);
            break;
        }
         
        case CASS_VALUE_TYPE_VARINT:
        case CASS_VALUE_TYPE_BLOB:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return false;
            
            cass_error = cass_statement_bind_bytes(statement, index, (cass_byte_t*)str_value.data(), str_value.size());
            break;
        }
            
        case CASS_VALUE_TYPE_BOOLEAN:
        {
            cass_bool_t bool_value = enif_is_identical(ATOMS.atomTrue, value) ? cass_true : cass_false;
            cass_error = cass_statement_bind_bool(statement, index, bool_value);
            break;
        }
            
        case CASS_VALUE_TYPE_FLOAT:
        case CASS_VALUE_TYPE_DOUBLE:
        {
            double val_double;
            if(!enif_get_double(env, value, &val_double))
                return false;
            
            if(type.type == CASS_VALUE_TYPE_FLOAT)
                cass_error = cass_statement_bind_float(statement, index, (float)val_double);
            else
                cass_error = cass_statement_bind_double(statement, index, val_double);
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
            
            cass_error = cass_statement_bind_inet(statement, index, inet);
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
            
            cass_error = cass_statement_bind_uuid(statement, index, uuid);
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
            
            cass_error = cass_statement_bind_decimal(statement, index, (cass_byte_t*)varint.data(), varint.size(), scale);
            break;
        }
        
        case CASS_VALUE_TYPE_MAP:
        case CASS_VALUE_TYPE_LIST:
        case CASS_VALUE_TYPE_SET:
        {
            CassError error = CASS_OK;
            
            CassCollection* collection = nif_list_to_cass_collection(env, value, type, &error);
            
            if(!collection)
                return false;
            
            if(error == CASS_OK)
                cass_error = cass_statement_bind_collection(statement, index, collection);
            else
                cass_error = error;
            
            cass_collection_free(collection);
            
            break;
        }
            
        default:
            cass_error = cass_statement_bind_null(statement, index);
            break;
    }
    
    return true;
}

bool bind_params_by_name(ErlNifEnv* env, CassStatement* statement, const ColumnsMap* columns_map, const std::string& key, ERL_NIF_TERM value, CassError& cass_error)
{
    ColumnsMap::const_iterator it = columns_map->find(key);
    
    if(it == columns_map->end())
        return false;
    
    if(enif_is_identical(value, ATOMS.atomNull))
    {
        cass_error = cass_statement_bind_null_by_name_n(statement, key.c_str(), key.length());
        return true;
    }
    
    switch (it->second.type)
    {
        case CASS_VALUE_TYPE_VARCHAR:
        case CASS_VALUE_TYPE_ASCII:
        case CASS_VALUE_TYPE_TEXT:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return false;
            
            cass_error = cass_statement_bind_string_by_name_n(statement, key.c_str(), key.length(), str_value.c_str(), str_value.length());
            break;
        }
            
        case CASS_VALUE_TYPE_INT:
        {
            int int_value = 0;
            
            if(!enif_get_int(env, value, &int_value ))
                return false;
            
            cass_error = cass_statement_bind_int32_by_name_n(statement, key.c_str(), key.length(), int_value);
            break;
        }
            
        case CASS_VALUE_TYPE_TIMESTAMP:
        case CASS_VALUE_TYPE_COUNTER:
        case CASS_VALUE_TYPE_BIGINT:
        {
            long long_value = 0;
            
            if(!enif_get_int64(env, value, &long_value ))
                return false;
            
            cass_error = cass_statement_bind_int64_by_name_n(statement, key.c_str(), key.length(), long_value);
            break;
        }
            
        case CASS_VALUE_TYPE_VARINT:
        case CASS_VALUE_TYPE_BLOB:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return false;
            
            cass_error = cass_statement_bind_bytes_by_name_n(statement, key.c_str(), key.length(), (cass_byte_t*)str_value.data(), str_value.size());
            break;
        }
            
        case CASS_VALUE_TYPE_BOOLEAN:
        {
            cass_bool_t bool_value = enif_is_identical(ATOMS.atomTrue, value) ? cass_true : cass_false;
            cass_error = cass_statement_bind_bool_by_name_n(statement, key.c_str(), key.length(), bool_value);
            break;
        }
            
        case CASS_VALUE_TYPE_FLOAT:
        case CASS_VALUE_TYPE_DOUBLE:
        {
            double val_double;
            if(!enif_get_double(env, value, &val_double))
                return false;
            
            if(it->second.type == CASS_VALUE_TYPE_FLOAT)
                cass_error = cass_statement_bind_float_by_name_n(statement, key.c_str(), key.length(), (float)val_double);
            else
                cass_error = cass_statement_bind_double_by_name_n(statement, key.c_str(), key.length(), val_double);
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
            
            cass_error = cass_statement_bind_inet_by_name_n(statement, key.c_str(), key.length(), inet);
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
            
            cass_error = cass_statement_bind_uuid_by_name_n(statement, key.c_str(), key.length(), uuid);
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
            
            cass_error = cass_statement_bind_decimal_by_name_n(statement, key.c_str(), key.length(), (cass_byte_t*)varint.data(), varint.size(), scale);
            break;
        }
            
        case CASS_VALUE_TYPE_MAP:
        case CASS_VALUE_TYPE_LIST:
        case CASS_VALUE_TYPE_SET:
        {
            CassError error = CASS_OK;
            
            CassCollection* collection = nif_list_to_cass_collection(env, value, it->second, &error);
            
            if(!collection)
                return false;
            
            if(error == CASS_OK)
                cass_error = cass_statement_bind_collection_by_name_n(statement, key.c_str(), key.length(), collection);
            else
                cass_error = error;
            
            cass_collection_free(collection);
            
            break;
        }

        default:
            cass_error = cass_statement_bind_null_by_name_n(statement, key.c_str(), key.length());
            break;
    }
    
    return true;
}

ERL_NIF_TERM bind_statement_params(ErlNifEnv* env, CassStatement* statement, const ColumnsMap* columns_map, ERL_NIF_TERM list)
{
    ERL_NIF_TERM head;
    const ERL_NIF_TERM *items;
    int arity;
    
    std::string column_name;
    
    while(enif_get_list_cell(env, list, &head, &list))
    {
        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return enif_make_badarg(env);
        
        if(!get_string(env, items[0], column_name))
            return enif_make_badarg(env);
        
        CassError cass_result;
        if(!bind_params_by_name(env, statement, columns_map, column_name, items[1], cass_result))
            return enif_make_badarg(env);
        
        if(cass_result != CASS_OK)
            return cass_error_to_nif_term(env, cass_result);
    }
    
    return ATOMS.atomOk;
}

CassStatement* get_statement(ErlNifEnv* env, ErlNifResourceType* resource_type, ERL_NIF_TERM arg)
{
    enif_cass_statement * enif_stm = NULL;
    
    if(!enif_get_resource(env, arg, resource_type, (void**) &enif_stm))
        return NULL;
    
    return enif_stm->statement;
}

ERL_NIF_TERM nif_cass_statement_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = (cassandra_data*) enif_priv_data(env);

    ERL_NIF_TERM queryTerm;
    ColumnsMap name_map;
    std::string query;
    CassConsistency consistencyLevel;
    
    if(enif_is_tuple(env, argv[0]))
    {
        const ERL_NIF_TERM *items;
        int arity;
        
        if(!enif_get_tuple(env, argv[0], &arity, &items) || arity != 2)
            return enif_make_badarg(env);
        
        queryTerm = items[0];
        int cLevel;
        
        if(!enif_get_int(env, items[1], &cLevel))
            return enif_make_badarg(env);
        
        consistencyLevel = static_cast<CassConsistency>(cLevel);
    }
    else
    {
        queryTerm = argv[0];
        consistencyLevel = data->defaultConsistencyLevel;
    }
    
    if(!get_string(env, queryTerm, query))
        return enif_make_badarg(env);
    
    ERL_NIF_TERM paramslist = argv[1];
    unsigned int params_length;
    
    if(!enif_get_list_length(env, paramslist, &params_length))
        return enif_make_badarg(env);

    CassStatement* stm = cass_statement_new_n(query.c_str(), query.length(), params_length);
    
    CassError cass_result = cass_statement_set_consistency(stm, consistencyLevel);
    
    if(cass_result != CASS_OK)
        return cass_error_to_nif_term(env, cass_result);
    
    if(params_length)
    {
        ERL_NIF_TERM head;
        const ERL_NIF_TERM *items;
        int arity;
        
        size_t index = 0;
        
        while(enif_get_list_cell(env, paramslist, &head, &paramslist))
        {
            if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
                return enif_make_badarg(env);
            
            SchemaColumn type = atom_to_schema_column(env, items[0]);
            
            if(type.valueType == CASS_VALUE_TYPE_UNKNOWN)
                return enif_make_badarg(env);
            
            if(!bind_params_by_index(env, stm, index++, type, items[1], cass_result))
                return enif_make_badarg(env);
            
            if(cass_result != CASS_OK)
                return cass_error_to_nif_term(env, cass_result);
        }
    }
    
    enif_cass_statement *enif_obj = (enif_cass_statement*) enif_alloc_resource(data->resCassStatement, sizeof(enif_cass_statement));
    
    if(enif_obj == NULL)
        return make_error(env, "enif_alloc_resource failed");
    
    enif_obj->statement = stm;
    enif_obj->columns_map = NULL;
    
    ERL_NIF_TERM term = enif_make_resource(env, enif_obj);
    enif_release_resource(enif_obj);

    return enif_make_tuple2(env, ATOMS.atomOk, term);
}


ERL_NIF_TERM nif_cass_statement_new(ErlNifEnv* env, ErlNifResourceType* resource_type, const CassPrepared* prep, CassConsistency consistency, const ColumnsMap* columns_map)
{
    enif_cass_statement *enif_obj = (enif_cass_statement*) enif_alloc_resource(resource_type, sizeof(enif_cass_statement));
    
    if(enif_obj == NULL)
        return make_error(env, "enif_alloc_resource failed");
    
    enif_obj->statement = cass_prepared_bind(prep);
    enif_obj->columns_map = columns_map;
    
    CassError cass_result = cass_statement_set_consistency(enif_obj->statement, consistency);
    
    if(cass_result != CASS_OK)
        return cass_error_to_nif_term(env, cass_result);
    
    ERL_NIF_TERM term = enif_make_resource(env, enif_obj);
    enif_release_resource(enif_obj);
    
    return term;
}

void nif_cass_statement_free(ErlNifEnv* env, void* obj)
{
    enif_cass_statement *data = (enif_cass_statement*) obj;
    
    if(data->statement != NULL)
        cass_statement_free(data->statement);
}

ERL_NIF_TERM nif_cass_statement_bind_parameters(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = (cassandra_data*) enif_priv_data(env);
    
    enif_cass_statement * enif_stm = NULL;
    
    if(!enif_get_resource(env, argv[0], data->resCassStatement, (void**) &enif_stm) || !enif_is_list(env, argv[1]))
        return enif_make_badarg(env);

    return bind_statement_params(env, enif_stm->statement, enif_stm->columns_map, argv[1]);
}
