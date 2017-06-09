#include "nif_collection.h"
#include "nif_utils.h"
#include "nif_tuple.h"
#include "nif_udt.h"
#include "uuid_serialization.h"
#include "constants.h"
#include "data_type.hpp"

static const int kIndexKey = 0;
static const int kIndexVal = 1;

ERL_NIF_TERM cass_collection_append_from_nif(ErlNifEnv* env, CassCollection* collection, const cass::DataType* data_type, ERL_NIF_TERM value)
{
    switch (data_type->value_type())
    {
        case CASS_VALUE_TYPE_VARCHAR:
        case CASS_VALUE_TYPE_ASCII:
        case CASS_VALUE_TYPE_TEXT:
        {
            ErlNifBinary bin;

            if(!get_bstring(env, value, &bin))
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_collection_append_string_n(collection, BIN_TO_STR(bin.data), bin.size));
        }

        case CASS_VALUE_TYPE_TINY_INT:
        {
            int int_value = 0;

            if(!enif_get_int(env, value, &int_value ))
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_collection_append_int8(collection, static_cast<cass_int8_t>(int_value)));
        }

        case CASS_VALUE_TYPE_SMALL_INT:
        {
            int int_value = 0;

            if(!enif_get_int(env, value, &int_value ))
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_collection_append_int16(collection, static_cast<cass_int16_t>(int_value)));
        }

        case CASS_VALUE_TYPE_INT:
        {
            int int_value = 0;

            if(!enif_get_int(env, value, &int_value ))
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_collection_append_int32(collection, int_value));
        }

        case CASS_VALUE_TYPE_DATE:
        {
            unsigned int uint_value = 0;

            if(!enif_get_uint(env, value, &uint_value ))
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_collection_append_uint32(collection, uint_value));
        }

        case CASS_VALUE_TYPE_TIME:
        case CASS_VALUE_TYPE_TIMESTAMP:
        case CASS_VALUE_TYPE_COUNTER:
        case CASS_VALUE_TYPE_BIGINT:
        {
            long long_value = 0;

            if(!enif_get_int64(env, value, &long_value ))
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_collection_append_int64(collection, long_value));
        }

        case CASS_VALUE_TYPE_VARINT:
        case CASS_VALUE_TYPE_BLOB:
        {
            ErlNifBinary bin;

            if(!get_bstring(env, value, &bin))
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_collection_append_bytes(collection, bin.data, bin.size));
        }

        case CASS_VALUE_TYPE_BOOLEAN:
        {
            cass_bool_t bool_value;

            if(!get_boolean(value, &bool_value))
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_collection_append_bool(collection, bool_value));
        }

        case CASS_VALUE_TYPE_FLOAT:
        case CASS_VALUE_TYPE_DOUBLE:
        {
            double val_double;
            if(!enif_get_double(env, value, &val_double))
                return make_badarg(env);

            if(data_type->value_type() == CASS_VALUE_TYPE_FLOAT)
                return cass_error_to_nif_term(env, cass_collection_append_float(collection, static_cast<float>(val_double)));
            else
                return cass_error_to_nif_term(env, cass_collection_append_double(collection, val_double));
        }

        case CASS_VALUE_TYPE_INET:
        {
            ErlNifBinary bin;

            if(!get_bstring(env, value, &bin))
                return make_badarg(env);

            CassInet inet;
            if(cass_inet_from_string_n(BIN_TO_STR(bin.data), bin.size, &inet) != CASS_OK)
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_collection_append_inet(collection, inet));
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

            return cass_error_to_nif_term(env, cass_collection_append_uuid(collection, uuid));
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

            return cass_error_to_nif_term(env, cass_collection_append_decimal(collection, varint.data, varint.size, scale));
        }

        case CASS_VALUE_TYPE_MAP:
        case CASS_VALUE_TYPE_LIST:
        case CASS_VALUE_TYPE_SET:
        {
            CassCollection* nested_collection = NULL;

            ERL_NIF_TERM result = nif_list_to_cass_collection(env, value, data_type, &nested_collection);

            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;

            CassError error = cass_collection_append_collection(collection, nested_collection);
            cass_collection_free(nested_collection);
            return cass_error_to_nif_term(env, error);
        }

        case CASS_VALUE_TYPE_TUPLE:
        {
            CassTuple* tuple = NULL;

            ERL_NIF_TERM result = nif_term_to_cass_tuple(env, value, data_type, &tuple);

            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;

            CassError error = cass_collection_append_tuple(collection, tuple);
            cass_tuple_free(tuple);
            return cass_error_to_nif_term(env, error);
        }

        case CASS_VALUE_TYPE_UDT:
        {
            CassUserType* nested_udt = NULL;

            ERL_NIF_TERM result = nif_term_to_cass_udt(env, value, data_type, &nested_udt);

            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;

            CassError error = cass_collection_append_user_type(collection, nested_udt);
            cass_user_type_free(nested_udt);
            return cass_error_to_nif_term(env, error);
        }

        //not implemented types
        default:
            return make_error(env, erlcass::kFailedToSetUnknownType);
    }
}

ERL_NIF_TERM populate_list_set_collection(ErlNifEnv* env, ERL_NIF_TERM list, CassCollection* collection, const cass::DataType* dt)
{
    ERL_NIF_TERM head;
    ERL_NIF_TERM item_term;

    while(enif_get_list_cell(env, list, &head, &list))
    {
        item_term = cass_collection_append_from_nif(env, collection, dt, head);

        if(!enif_is_identical(item_term, ATOMS.atomOk))
            return item_term;
    }

    return ATOMS.atomOk;
}

ERL_NIF_TERM populate_map_collection(ErlNifEnv* env, ERL_NIF_TERM list, CassCollection* collection, const cass::DataType* kt, const cass::DataType* vt)
{
    ERL_NIF_TERM head;
    ERL_NIF_TERM item_term;
    const ERL_NIF_TERM *items;
    int arity;

    while(enif_get_list_cell(env, list, &head, &list))
    {
        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return make_badarg(env);

        //add key
        item_term = cass_collection_append_from_nif(env, collection, kt, items[0]);

        if(!enif_is_identical(item_term, ATOMS.atomOk))
            return item_term;

        //add value

        item_term = cass_collection_append_from_nif(env, collection, vt, items[1]);

        if(!enif_is_identical(item_term, ATOMS.atomOk))
            return item_term;
    }

    return ATOMS.atomOk;
}

ERL_NIF_TERM nif_list_to_cass_collection(ErlNifEnv* env, ERL_NIF_TERM list, const cass::DataType* data_type, CassCollection ** col)
{
    const cass::CompositeType* ct = static_cast<const cass::CompositeType*>(data_type);

    unsigned int length;

    if(!enif_get_list_length(env, list, &length))
        return make_badarg(env);

    CassCollection* collection = cass_collection_new(static_cast<CassCollectionType>(data_type->value_type()), length);

    ERL_NIF_TERM return_value;

    if(data_type->value_type() != CASS_VALUE_TYPE_MAP)
        return_value = populate_list_set_collection(env, list, collection, ct->types().at(kIndexKey).get());
    else
        return_value = populate_map_collection(env, list, collection, ct->types().at(kIndexKey).get(), ct->types().at(kIndexVal).get());

    if(!enif_is_identical(return_value, ATOMS.atomOk))
        cass_collection_free(collection);
    else
        *col = collection;

    return return_value;
}
