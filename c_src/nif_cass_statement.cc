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
#include "constants.h"

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
        const cass::CompositeType* collection_type = static_cast<const cass::CompositeType*>(data_type);
        
        for(cass::DataType::Vec::const_iterator it = collection_type->types().begin(); it != collection_type->types().end(); ++it)
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
            ErlNifBinary bin;
            
            if(!get_bstring(env, value, &bin))
                return make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_statement_bind_string_n(statement, index, BIN_TO_STR(bin.data), bin.size));
        }

        case CASS_VALUE_TYPE_TINY_INT:
        {
            int int_value = 0;
            
            if(!enif_get_int(env, value, &int_value ))
                return make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_statement_bind_int8(statement, index, static_cast<cass_int8_t>(int_value)));
        }
            
        case CASS_VALUE_TYPE_SMALL_INT:
        {
            int int_value = 0;
            
            if(!enif_get_int(env, value, &int_value ))
                return make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_statement_bind_int16(statement, index, static_cast<cass_int16_t>(int_value)));
        }
            
        case CASS_VALUE_TYPE_INT:
        {
            int int_value = 0;
            
            if(!enif_get_int(env, value, &int_value ))
                return make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_statement_bind_int32(statement, index, int_value));
        }
            
        case CASS_VALUE_TYPE_DATE:
        {
            unsigned int uint_value = 0;
            
            if(!enif_get_uint(env, value, &uint_value ))
                return make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_statement_bind_uint32(statement, index, uint_value));
        }
            
        case CASS_VALUE_TYPE_TIME:
        case CASS_VALUE_TYPE_TIMESTAMP:
        case CASS_VALUE_TYPE_COUNTER:
        case CASS_VALUE_TYPE_BIGINT:
        {
            long long_value = 0;
            
            if(!enif_get_int64(env, value, &long_value ))
                return make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_statement_bind_int64(statement, index, long_value));
        }
         
        case CASS_VALUE_TYPE_VARINT:
        case CASS_VALUE_TYPE_BLOB:
        {
            ErlNifBinary bin;
            
            if(!get_bstring(env, value, &bin))
                return make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_statement_bind_bytes(statement, index, bin.data, bin.size));
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
                return make_badarg(env);
            
            if(type.type == CASS_VALUE_TYPE_FLOAT)
                return cass_error_to_nif_term(env, cass_statement_bind_float(statement, index, static_cast<float>(val_double)));
            else
                return cass_error_to_nif_term(env, cass_statement_bind_double(statement, index, val_double));
        }
            
        case CASS_VALUE_TYPE_INET:
        {
            ErlNifBinary bin;
            
            if(!get_bstring(env, value, &bin))
                return make_badarg(env);
            
            CassInet inet;
            if(cass_inet_from_string_n(BIN_TO_STR(bin.data), bin.size, &inet) != CASS_OK)
                return make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_statement_bind_inet(statement, index, inet));
        }
            
        case CASS_VALUE_TYPE_TIMEUUID:
        case CASS_VALUE_TYPE_UUID:
        {
            ErlNifBinary bin;
            
            if(!get_bstring(env, value, &bin))
                return make_badarg(env);
            
            CassUuid uuid;
            if(erlcass::cass_uuid_from_string_n(BIN_TO_STR(bin.data), bin.size, &uuid) != CASS_OK)
                return make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_statement_bind_uuid(statement, index, uuid));
        }
            
        case CASS_VALUE_TYPE_DECIMAL:
        {
            const ERL_NIF_TERM *items;
            int arity;
            
            if(!enif_get_tuple(env, value, &arity, &items) || arity != 2)
                return make_badarg(env);
            
            ErlNifBinary varint;
            int scale;
            
            if(!get_bstring(env, items[0], &varint) || !enif_get_int(env, items[1], &scale))
                return make_badarg(env);
            
            return cass_error_to_nif_term(env, cass_statement_bind_decimal(statement, index, varint.data, varint.size, scale));
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
            return make_error(env, erlcass::kBindFailedUnknownColumnType);
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
        
        cass::IndexVec indices;
        ErlNifBinary column_name;
        const ERL_NIF_TERM *items;
        int arity;
        
        while(enif_get_list_cell(env, list, &head, &list))
        {
            if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
                return make_badarg(env);
            
            if(!get_bstring(env, items[0], &column_name))
                return make_badarg(env);
                        
            if(result->metadata()->get_indices(cass::StringRef(BIN_TO_STR(column_name.data), column_name.size), &indices) == 0)
                return make_badarg(env);
            
            size_t index = indices[0];
            
            SchemaColumn sc = get_schema_column(result->metadata()->get_column_definition(index).data_type.get());
            ERL_NIF_TERM result = bind_param_by_index(env, statement, index, sc, items[1]);
                
            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;
        }
    }
    else
    {
        //bind by index
        
        size_t index = 0;
        
        while(enif_get_list_cell(env, list, &head, &list))
        {
            if(index > result->metadata()->column_count())
                return make_badarg(env);
            
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

    QueryTerm q(ConsistencyLevelOptions(data->defaultConsistencyLevel, CASS_CONSISTENCY_ANY));

    ERL_NIF_TERM parse_result = parse_query_term(env, argv[0], &q);

    if(!enif_is_identical(ATOMS.atomOk, parse_result))
        return parse_result;

    unsigned int params_length = 0;
    
    if(argc == 2)
    {
        if(!enif_get_list_length(env, argv[1], &params_length))
            return make_badarg(env);
    }

    CassStatement* stm = cass_statement_new_n(BIN_TO_STR(q.query.data), q.query.size, params_length);
    
    CassError cass_result = cass_statement_set_consistency(stm, q.consistency.cl);
    
    if(cass_result != CASS_OK)
        return cass_error_to_nif_term(env, cass_result);
    
    if(q.consistency.serial_cl != CASS_CONSISTENCY_ANY)
    {
        cass_result = cass_statement_set_serial_consistency(stm, q.consistency.serial_cl);
        
        if(cass_result != CASS_OK)
            return cass_error_to_nif_term(env, cass_result);
    }
    
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
                return make_badarg(env);
            
            SchemaColumn type = atom_to_schema_column(env, items[0]);
            
            if(type.type == CASS_VALUE_TYPE_UNKNOWN)
                return make_badarg(env);
            
            ERL_NIF_TERM result = bind_param_by_index(env, stm, index, type, items[1]);
            
            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;
            
            index++;
        }
    }
    
    enif_cass_statement *enif_obj = static_cast<enif_cass_statement*>(enif_alloc_resource(data->resCassStatement, sizeof(enif_cass_statement)));
    
    if(enif_obj == NULL)
        return make_error(env, erlcass::kFailedToAllocResourceMsg);
    
    enif_obj->statement = stm;
    
    ERL_NIF_TERM term = enif_make_resource(env, enif_obj);
    enif_release_resource(enif_obj);

    return enif_make_tuple2(env, ATOMS.atomOk, term);
}


ERL_NIF_TERM nif_cass_statement_new(ErlNifEnv* env, ErlNifResourceType* resource_type, const CassPrepared* prep, const ConsistencyLevelOptions& consistency)
{
    enif_cass_statement *enif_obj = static_cast<enif_cass_statement*>(enif_alloc_resource(resource_type, sizeof(enif_cass_statement)));
    
    if(enif_obj == NULL)
        return make_error(env, erlcass::kFailedToAllocResourceMsg);
    
    enif_obj->statement = cass_prepared_bind(prep);
    
    CassError cass_result = cass_statement_set_consistency(enif_obj->statement, consistency.cl);
    
    if(cass_result != CASS_OK)
        return cass_error_to_nif_term(env, cass_result);
    
    if(consistency.serial_cl != CASS_CONSISTENCY_ANY )
    {
        cass_result = cass_statement_set_serial_consistency(enif_obj->statement, consistency.serial_cl);
        
        if(cass_result != CASS_OK)
            return cass_error_to_nif_term(env, cass_result);
    }
    
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
        return make_badarg(env);
    
    int bind_type;
    
    if(!enif_get_int(env, argv[1], &bind_type) || (bind_type != BIND_BY_INDEX && bind_type != BIND_BY_NAME))
        return make_badarg(env);

    return bind_prepared_statement_params(env, enif_stm->statement, bind_type, argv[2]);
}
