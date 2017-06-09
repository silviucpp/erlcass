#include "nif_udt.h"
#include "nif_tuple.h"
#include "nif_utils.h"
#include "nif_collection.h"
#include "uuid_serialization.h"
#include "cassandra.h"
#include "constants.h"
#include "data_type.hpp"
#include <memory>

ERL_NIF_TERM cass_udt_set_from_nif(ErlNifEnv* env, CassUserType* udt, size_t index, const cass::DataType* data_type, ERL_NIF_TERM value)
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

            return cass_error_to_nif_term(env, cass_user_type_set_string_n(udt, index, BIN_TO_STR(bin.data), bin.size));
        }

        case CASS_VALUE_TYPE_TINY_INT:
        {
            int int_value = 0;

            if(!enif_get_int(env, value, &int_value ))
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_user_type_set_int8(udt, index, static_cast<cass_int8_t>(int_value)));
        }

        case CASS_VALUE_TYPE_SMALL_INT:
        {
            int int_value = 0;

            if(!enif_get_int(env, value, &int_value ))
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_user_type_set_int16(udt, index, static_cast<cass_int16_t>(int_value)));
        }

        case CASS_VALUE_TYPE_INT:
        {
            int int_value = 0;

            if(!enif_get_int(env, value, &int_value ))
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_user_type_set_int32(udt, index, int_value));
        }

        case CASS_VALUE_TYPE_DATE:
        {
            unsigned int uint_value = 0;

            if(!enif_get_uint(env, value, &uint_value ))
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_user_type_set_uint32(udt, index, uint_value));
        }

        case CASS_VALUE_TYPE_TIME:
        case CASS_VALUE_TYPE_TIMESTAMP:
        case CASS_VALUE_TYPE_COUNTER:
        case CASS_VALUE_TYPE_BIGINT:
        {
            long long_value = 0;

            if(!enif_get_int64(env, value, &long_value ))
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_user_type_set_int64(udt, index, long_value));
        }

        case CASS_VALUE_TYPE_VARINT:
        case CASS_VALUE_TYPE_BLOB:
        {
            ErlNifBinary bin;

            if(!get_bstring(env, value, &bin))
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_user_type_set_bytes(udt, index, bin.data, bin.size));
        }

        case CASS_VALUE_TYPE_BOOLEAN:
        {
            cass_bool_t bool_value;

            if(!get_boolean(value, &bool_value))
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_user_type_set_bool(udt, index, bool_value));
        }

        case CASS_VALUE_TYPE_FLOAT:
        case CASS_VALUE_TYPE_DOUBLE:
        {
            double val_double;
            if(!enif_get_double(env, value, &val_double))
                return make_badarg(env);

            if(data_type->value_type() == CASS_VALUE_TYPE_FLOAT)
                return cass_error_to_nif_term(env, cass_user_type_set_float(udt, index, static_cast<float>(val_double)));
            else
                return cass_error_to_nif_term(env, cass_user_type_set_double(udt, index, val_double));
        }

        case CASS_VALUE_TYPE_INET:
        {
            ErlNifBinary bin;

            if(!get_bstring(env, value, &bin))
                return make_badarg(env);

            CassInet inet;
            if(cass_inet_from_string_n(BIN_TO_STR(bin.data), bin.size, &inet) != CASS_OK)
                return make_badarg(env);

            return cass_error_to_nif_term(env, cass_user_type_set_inet(udt, index, inet));
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

            return cass_error_to_nif_term(env, cass_user_type_set_uuid(udt, index, uuid));
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

            return cass_error_to_nif_term(env, cass_user_type_set_decimal(udt, index, varint.data, varint.size, scale));
        }

        case CASS_VALUE_TYPE_MAP:
        case CASS_VALUE_TYPE_LIST:
        case CASS_VALUE_TYPE_SET:
        {
            CassCollection* collection = NULL;

            ERL_NIF_TERM result = nif_list_to_cass_collection(env, value, data_type, &collection);

            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;

            CassError error = cass_user_type_set_collection(udt, index, collection);
            cass_collection_free(collection);
            return cass_error_to_nif_term(env, error);
        }

        case CASS_VALUE_TYPE_TUPLE:
        {
            CassTuple* nested_tuple = NULL;

            ERL_NIF_TERM result = nif_term_to_cass_tuple(env, value, data_type, &nested_tuple);

            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;

            CassError error = cass_user_type_set_tuple(udt, index, nested_tuple);
            cass_tuple_free(nested_tuple);
            return cass_error_to_nif_term(env, error);
        }

        case CASS_VALUE_TYPE_UDT:
        {
            CassUserType* nested_udt = NULL;

            ERL_NIF_TERM result = nif_term_to_cass_udt(env, value, data_type, &nested_udt);

            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;

            CassError error = cass_user_type_set_user_type(udt, index, nested_udt);
            cass_user_type_free(nested_udt);
            return cass_error_to_nif_term(env, error);
        }

        default:
            return make_error(env, erlcass::kFailedToSetUnknownType);
    }
}

ERL_NIF_TERM nif_term_to_cass_udt(ErlNifEnv* env, ERL_NIF_TERM term, const cass::DataType* dt, CassUserType** udt)
{
    unsigned int length;

    if(!enif_get_list_length(env, term, &length) || length < 1)
        return make_badarg(env);

    scoped_ptr(data_type, CassDataType, cass_data_type_new_udt(length), cass_data_type_free);

    const cass::UserType* ut = static_cast<const cass::UserType*>(dt);

    for(auto it = ut->fields().begin(); it != ut->fields().end(); ++it)
    {
        CassError err = cass_data_type_add_sub_value_type_by_name(data_type.get(), it->name.c_str(), it->type->value_type());

        if(err != CASS_OK)
            return cass_error_to_nif_term(env, err);
    }

    scoped_ptr(utv, CassUserType, cass_user_type_new_from_data_type(data_type.get()), cass_user_type_free);

    ERL_NIF_TERM head;
    const ERL_NIF_TERM *items;
    int arity;

    while(enif_get_list_cell(env, term, &head, &term))
    {
        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return make_badarg(env);

        ErlNifBinary bin;

        if(!get_bstring(env, items[0], &bin))
            return make_badarg(env);

        cass::IndexVec indices;

        if(ut->get_indices(cass::StringRef(BIN_TO_STR(bin.data), bin.size), &indices) == 0)
            return make_badarg(env);

        size_t index = indices[0];
        const cass::DataType* type = ut->fields().at(index).type.get();

        ERL_NIF_TERM item_term = cass_udt_set_from_nif(env, utv.get(), index, type, items[1]);

        if(!enif_is_identical(item_term, ATOMS.atomOk))
            return item_term;
    }

    *udt = utv.release();
    return ATOMS.atomOk;
}
