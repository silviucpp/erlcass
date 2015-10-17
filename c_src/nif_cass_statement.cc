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
#include "nif_tuple.h"
#include "uuid_serialization.h"
#include "execute_request.hpp"
#include "external_types.hpp"

#define BIND_BY_INDEX 1
#define BIND_BY_NAME  2

struct enif_cass_statement
{
    CassStatement* statement;
};

SchemaColumn get_schema_column(const cass::DataType* data_type)
{
    SchemaColumn sc(data_type->value_type());
    
    if(data_type->is_collection() || data_type->is_tuple())
    {
        const cass::SubTypesDataType* collection_type = static_cast<const cass::SubTypesDataType*>(data_type);
        
        for(cass::DataTypeVec::const_iterator it = collection_type->types().begin(); it != collection_type->types().end(); ++it)
            sc.subtypes.push_back(get_schema_column((*it).get()));
    }

    return sc;
}

ERL_NIF_TERM bind_param_by_index(ErlNifEnv* env, CassStatement* statement, size_t index, const SchemaColumn& type, ERL_NIF_TERM value)
{
    if(enif_is_identical(value, ATOMS.atomNull))
        return cass_error_to_nif_term(env, cass_statement_bind_null(statement, index));
    
    switch (type.type)
    {
            
        case CASS_VALUE_TYPE_VARCHAR:
        case CASS_VALUE_TYPE_ASCII:
        case CASS_VALUE_TYPE_TEXT:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_statement_bind_string_n(statement, index, str_value.c_str(), str_value.length()));
        }
            
        case CASS_VALUE_TYPE_INT:
        {
            int int_value = 0;
            
            if(!enif_get_int(env, value, &int_value ))
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_statement_bind_int32(statement, index, int_value));
        }
            
        case CASS_VALUE_TYPE_TIMESTAMP:
        case CASS_VALUE_TYPE_COUNTER:
        case CASS_VALUE_TYPE_BIGINT:
        {
            long long_value = 0;
            
            if(!enif_get_int64(env, value, &long_value ))
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_statement_bind_int64(statement, index, long_value));
        }
         
        case CASS_VALUE_TYPE_VARINT:
        case CASS_VALUE_TYPE_BLOB:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return enif_make_badarg(env);
            
            const cass_byte_t* bytes = reinterpret_cast<const cass_byte_t*>(str_value.data());
            return cass_error_to_nif_term(env, cass_statement_bind_bytes(statement, index, bytes, str_value.size()));
        }
            
        case CASS_VALUE_TYPE_BOOLEAN:
        {
            cass_bool_t bool_value = static_cast<cass_bool_t>(enif_is_identical(ATOMS.atomTrue, value));
            return cass_error_to_nif_term(env, cass_statement_bind_bool(statement, index, bool_value));
        }
            
        case CASS_VALUE_TYPE_FLOAT:
        case CASS_VALUE_TYPE_DOUBLE:
        {
            double val_double;
            if(!enif_get_double(env, value, &val_double))
                return enif_make_badarg(env);
            
            if(type.type == CASS_VALUE_TYPE_FLOAT)
                return cass_error_to_nif_term(env, cass_statement_bind_float(statement, index, static_cast<float>(val_double)));
            else
                return cass_error_to_nif_term(env, cass_statement_bind_double(statement, index, val_double));
        }
            
        case CASS_VALUE_TYPE_INET:
        {
            std::string str_value;
            
            if(!get_string(env, value, str_value))
                return enif_make_badarg(env);
            
            CassInet inet;
            if(cass_inet_from_string_n(str_value.c_str(), str_value.length(), &inet) != CASS_OK)
                return enif_make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_statement_bind_inet(statement, index, inet));
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
            
            return cass_error_to_nif_term(env, cass_statement_bind_uuid(statement, index, uuid));
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
            
            const cass_byte_t* variant_bytes = reinterpret_cast<const cass_byte_t*>(varint.data());
            return cass_error_to_nif_term(env, cass_statement_bind_decimal(statement, index, variant_bytes, varint.size(), scale));
        }
        
        case CASS_VALUE_TYPE_MAP:
        case CASS_VALUE_TYPE_LIST:
        case CASS_VALUE_TYPE_SET:
        {
            CassCollection* collection = NULL;
            
            ERL_NIF_TERM result = nif_list_to_cass_collection(env, value, type, &collection);
            
            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;
            
            CassError error = cass_statement_bind_collection(statement, index, collection);
            cass_collection_free(collection);
            return cass_error_to_nif_term(env, error);
        }
            
        case CASS_VALUE_TYPE_TUPLE:
        {
            CassTuple* tuple = NULL;
            
            ERL_NIF_TERM result = nif_term_to_cass_tuple(env, value, type, &tuple);
            
            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;
            
            CassError error = cass_statement_bind_tuple(statement, index, tuple);
            cass_tuple_free(tuple);
            return cass_error_to_nif_term(env, error);
        }

        //not implemented data types
        default:
            return make_error(env, "bind failed: not implemented column type");
    }
    
}

ERL_NIF_TERM bind_prepared_statement_params(ErlNifEnv* env, CassStatement* statement, int type, ERL_NIF_TERM list)
{
    ERL_NIF_TERM head;

    cass::Statement* stm = static_cast<cass::Statement*>(statement);
    const cass::ResultResponse* result = static_cast<cass::ExecuteRequest*>(stm)->prepared()->result().get();
    
    if(type == BIND_BY_NAME)
    {
        //bind by name -> {name, value}
        
        std::string column_name;
        const ERL_NIF_TERM *items;
        int arity;
        
        while(enif_get_list_cell(env, list, &head, &list))
        {
            if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
                return enif_make_badarg(env);
            
            if(!get_string(env, items[0], column_name))
                return enif_make_badarg(env);
            
            cass::IndexVec indices;
            
            result->metadata()->get_indices(column_name, &indices);
            
            for (cass::IndexVec::const_iterator it = indices.begin(); it != indices.end(); ++it)
            {
                SchemaColumn sc = get_schema_column(result->metadata()->get_column_definition(*it).data_type.get());
                
                ERL_NIF_TERM result = bind_param_by_index(env, statement, *it, sc, items[1]);
                
                if(!enif_is_identical(result, ATOMS.atomOk))
                    return result;
            }
        }
    }
    else
    {
        //bind by index
        
        size_t index = 0;
        
        while(enif_get_list_cell(env, list, &head, &list))
        {
            if(index > result->metadata()->column_count())
                return enif_make_badarg(env);
            
            const cass::ColumnDefinition def = result->metadata()->get_column_definition(index);
            
            SchemaColumn sc = get_schema_column(def.data_type.get());
            
            ERL_NIF_TERM result = bind_param_by_index(env, statement, index, sc, head);
            
            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;
            
            index++;
        }
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
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));

    ERL_NIF_TERM queryTerm;
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
    
    unsigned int params_length = 0;
    
    if(argc == 2)
    {
        if(!enif_get_list_length(env, argv[1], &params_length))
            return enif_make_badarg(env);
    }

    CassStatement* stm = cass_statement_new_n(query.c_str(), query.length(), params_length);
    
    CassError cass_result = cass_statement_set_consistency(stm, consistencyLevel);
    
    if(cass_result != CASS_OK)
        return cass_error_to_nif_term(env, cass_result);
    
    if(params_length)
    {
        ERL_NIF_TERM paramslist = argv[1];
        ERL_NIF_TERM head;
        const ERL_NIF_TERM *items;
        int arity;
        
        size_t index = 0;
        
        while(enif_get_list_cell(env, paramslist, &head, &paramslist))
        {
            if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
                return enif_make_badarg(env);
            
            SchemaColumn type = atom_to_schema_column(env, items[0]);
            
            if(type.type == CASS_VALUE_TYPE_UNKNOWN)
                return enif_make_badarg(env);
            
            ERL_NIF_TERM result = bind_param_by_index(env, stm, index, type, items[1]);
            
            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;
            
            index++;
        }
    }
    
    enif_cass_statement *enif_obj = static_cast<enif_cass_statement*>(enif_alloc_resource(data->resCassStatement, sizeof(enif_cass_statement)));
    
    if(enif_obj == NULL)
        return make_error(env, "enif_alloc_resource failed");
    
    enif_obj->statement = stm;
    
    ERL_NIF_TERM term = enif_make_resource(env, enif_obj);
    enif_release_resource(enif_obj);

    return enif_make_tuple2(env, ATOMS.atomOk, term);
}


ERL_NIF_TERM nif_cass_statement_new(ErlNifEnv* env, ErlNifResourceType* resource_type, const CassPrepared* prep, CassConsistency consistency)
{
    enif_cass_statement *enif_obj = static_cast<enif_cass_statement*>(enif_alloc_resource(resource_type, sizeof(enif_cass_statement)));
    
    if(enif_obj == NULL)
        return make_error(env, "enif_alloc_resource failed");
    
    enif_obj->statement = cass_prepared_bind(prep);
    
    CassError cass_result = cass_statement_set_consistency(enif_obj->statement, consistency);
    
    if(cass_result != CASS_OK)
        return cass_error_to_nif_term(env, cass_result);
    
    ERL_NIF_TERM term = enif_make_resource(env, enif_obj);
    enif_release_resource(enif_obj);
    
    return term;
}

void nif_cass_statement_free(ErlNifEnv* env, void* obj)
{
    enif_cass_statement *data = static_cast<enif_cass_statement*>(obj);
    
    if(data->statement != NULL)
        cass_statement_free(data->statement);
}

ERL_NIF_TERM nif_cass_statement_bind_parameters(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    cassandra_data* data = static_cast<cassandra_data*>(enif_priv_data(env));
    
    enif_cass_statement * enif_stm = NULL;
    
    if(!enif_get_resource(env, argv[0], data->resCassStatement, (void**) &enif_stm) || !enif_is_list(env, argv[2]))
        return enif_make_badarg(env);
    
    int bind_type;
    
    if(!enif_get_int(env, argv[1], &bind_type) || (bind_type != BIND_BY_INDEX && bind_type != BIND_BY_NAME))
        return enif_make_badarg(env);

    return bind_prepared_statement_params(env, enif_stm->statement, bind_type, argv[2]);
}
