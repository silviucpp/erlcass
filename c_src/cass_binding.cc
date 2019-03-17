#include "cass_binding.h"
#include "erlcass.h"
#include "nif_utils.h"
#include "fun_bindings.h"
#include "uuid_serialization.h"
#include "constants.h"
#include "data_type.hpp"
#include "macros.h"

#include <memory>
#include <functional>

#define MAX_ATOM_SIZE 256

static const int kIndexKey = 0;
static const int kIndexVal = 1;

template <typename T> struct set_data_functions
{
    std::function<CassError(T, size_t, const char* , size_t)> set_string;
    std::function<CassError(T, size_t, cass_int8_t)> set_int8;
    std::function<CassError(T, size_t, cass_int16_t)> set_int16;
    std::function<CassError(T, size_t, cass_int32_t)> set_int32;
    std::function<CassError(T, size_t, cass_int64_t)> set_int64;
    std::function<CassError(T, size_t, cass_uint32_t)> set_uint32;
    std::function<CassError(T, size_t, const cass_byte_t*, size_t)> set_bytes;
    std::function<CassError(T, size_t, cass_bool_t)> set_bool;
    std::function<CassError(T, size_t, cass_float_t)> set_float;
    std::function<CassError(T, size_t, cass_double_t)> set_double;
    std::function<CassError(T, size_t, CassInet)> set_inet;
    std::function<CassError(T, size_t, CassUuid)> set_uuid;
    std::function<CassError(T, size_t, const cass_byte_t*, size_t, int)> set_decimal;
    std::function<CassError(T, size_t, const CassCollection*)> set_collection;
    std::function<CassError(T, size_t, const CassTuple*)> set_tuple;
    std::function<CassError(T, size_t, const CassUserType*)> set_user_type;
};

const set_data_functions<CassStatement*> kCassStatementFuns = {
    cass_statement_bind_string_n,
    cass_statement_bind_int8,
    cass_statement_bind_int16,
    cass_statement_bind_int32,
    cass_statement_bind_int64,
    cass_statement_bind_uint32,
    cass_statement_bind_bytes,
    cass_statement_bind_bool,
    cass_statement_bind_float,
    cass_statement_bind_double,
    cass_statement_bind_inet,
    cass_statement_bind_uuid,
    cass_statement_bind_decimal,
    cass_statement_bind_collection,
    cass_statement_bind_tuple,
    cass_statement_bind_user_type
};

const set_data_functions<CassCollection*> kCassCollectionFuns = {
    erlcass_cass_collection_append_string_n,
    erlcass_cass_collection_append_int8,
    erlcass_cass_collection_append_int16,
    erlcass_cass_collection_append_int32,
    erlcass_cass_collection_append_int64,
    erlcass_cass_collection_append_uint32,
    erlcass_cass_collection_append_bytes,
    erlcass_cass_collection_append_bool,
    erlcass_cass_collection_append_float,
    erlcass_cass_collection_append_double,
    erlcass_cass_collection_append_inet,
    erlcass_cass_collection_append_uuid,
    erlcass_cass_collection_append_decimal,
    erlcass_cass_collection_append_collection,
    erlcass_cass_collection_append_tuple,
    erlcass_cass_collection_append_user_type
};

const set_data_functions<CassTuple*> kCassTupleFuns = {
    cass_tuple_set_string_n,
    cass_tuple_set_int8,
    cass_tuple_set_int16,
    cass_tuple_set_int32,
    cass_tuple_set_int64,
    cass_tuple_set_uint32,
    cass_tuple_set_bytes,
    cass_tuple_set_bool,
    cass_tuple_set_float,
    cass_tuple_set_double,
    cass_tuple_set_inet,
    cass_tuple_set_uuid,
    cass_tuple_set_decimal,
    cass_tuple_set_collection,
    cass_tuple_set_tuple,
    cass_tuple_set_user_type
};

const set_data_functions<CassUserType*> kCassUserTypeFuns = {
    cass_user_type_set_string_n,
    cass_user_type_set_int8,
    cass_user_type_set_int16,
    cass_user_type_set_int32,
    cass_user_type_set_int64,
    cass_user_type_set_uint32,
    cass_user_type_set_bytes,
    cass_user_type_set_bool,
    cass_user_type_set_float,
    cass_user_type_set_double,
    cass_user_type_set_inet,
    cass_user_type_set_uuid,
    cass_user_type_set_decimal,
    cass_user_type_set_collection,
    cass_user_type_set_tuple,
    cass_user_type_set_user_type
};

ERL_NIF_TERM nif_term_to_cass_udt(ErlNifEnv* env, ERL_NIF_TERM term, const datastax::internal::core::DataType* data_type, CassUserType** udt);
ERL_NIF_TERM nif_term_to_cass_tuple(ErlNifEnv* env, ERL_NIF_TERM term, const datastax::internal::core::DataType* data_type, CassTuple** tp);
ERL_NIF_TERM nif_list_to_cass_collection(ErlNifEnv* env, ERL_NIF_TERM list, const datastax::internal::core::DataType* data_type, CassCollection ** col);

template <typename T> ERL_NIF_TERM cass_set_from_nif(ErlNifEnv* env, T obj, size_t index, set_data_functions<T> fun, const datastax::internal::core::DataType* data_type, ERL_NIF_TERM value)
{
    switch (data_type->value_type())
    {
        case CASS_VALUE_TYPE_VARCHAR:
        case CASS_VALUE_TYPE_ASCII:
        case CASS_VALUE_TYPE_TEXT:
        {
            ErlNifBinary bin;
            if(get_bstring(env, value, &bin)) {
                return cass_error_to_nif_term(env, fun.set_string(obj, index, BIN_TO_STR(bin.data), bin.size));
            } else {
                char atom[MAX_ATOM_SIZE];
                int atom_size = 0;
                if((atom_size = get_atom(env, value, atom, MAX_ATOM_SIZE)) && atom_size > 0) {
                    return cass_error_to_nif_term(env, fun.set_string(obj, index, atom, atom_size - 1));
                }
            }

            return make_badarg(env);
        }

        case CASS_VALUE_TYPE_TINY_INT:
        {
            int int_value = 0;

            if(!enif_get_int(env, value, &int_value ))
                return make_badarg(env);

            return cass_error_to_nif_term(env, fun.set_int8(obj, index, static_cast<cass_int8_t>(int_value)));
        }

        case CASS_VALUE_TYPE_SMALL_INT:
        {
            int int_value = 0;

            if(!enif_get_int(env, value, &int_value ))
                return make_badarg(env);

            return cass_error_to_nif_term(env, fun.set_int16(obj, index, static_cast<cass_int16_t>(int_value)));
        }

        case CASS_VALUE_TYPE_INT:
        {
            int int_value = 0;

            if(!enif_get_int(env, value, &int_value ))
                return make_badarg(env);

            return cass_error_to_nif_term(env, fun.set_int32(obj, index, int_value));
        }

        case CASS_VALUE_TYPE_DATE:
        {
            unsigned int uint_value = 0;

            if(!enif_get_uint(env, value, &uint_value ))
                return make_badarg(env);

            return cass_error_to_nif_term(env, fun.set_uint32(obj, index, uint_value));
        }

        case CASS_VALUE_TYPE_TIME:
        case CASS_VALUE_TYPE_TIMESTAMP:
        case CASS_VALUE_TYPE_COUNTER:
        case CASS_VALUE_TYPE_BIGINT:
        {
            long long_value = 0;

            if(!enif_get_int64(env, value, &long_value ))
                return make_badarg(env);

            return cass_error_to_nif_term(env, fun.set_int64(obj, index, long_value));
        }

        case CASS_VALUE_TYPE_VARINT:
        case CASS_VALUE_TYPE_BLOB:
        {
            ErlNifBinary bin;

            if(!get_bstring(env, value, &bin))
                return make_badarg(env);

            return cass_error_to_nif_term(env, fun.set_bytes(obj, index, bin.data, bin.size));
        }

        case CASS_VALUE_TYPE_BOOLEAN:
        {
            cass_bool_t bool_value;

            if(!get_boolean(value, &bool_value))
                return make_badarg(env);

            return cass_error_to_nif_term(env, fun.set_bool(obj, index, bool_value));
        }

        case CASS_VALUE_TYPE_FLOAT:
        case CASS_VALUE_TYPE_DOUBLE:
        {
            double val_double = 0;
            long val_long = 0;
            bool success = false;
            if(enif_get_double(env, value, &val_double)) {
                success = true;
            } else if (enif_get_int64(env, value, &val_long)) {
                val_double = static_cast<double>(val_long);
                success = true;
            }

            if(success) {
                if(data_type->value_type() == CASS_VALUE_TYPE_FLOAT)
                    return cass_error_to_nif_term(env, fun.set_float(obj, index, static_cast<float>(val_double)));
                else
                    return cass_error_to_nif_term(env, fun.set_double(obj, index, val_double));
            }

            return make_badarg(env);
        }

        case CASS_VALUE_TYPE_INET:
        {
            ErlNifBinary bin;

            if(!get_bstring(env, value, &bin))
                return make_badarg(env);

            CassInet inet;
            if(cass_inet_from_string_n(BIN_TO_STR(bin.data), bin.size, &inet) != CASS_OK)
                return make_badarg(env);

            return cass_error_to_nif_term(env, fun.set_inet(obj, index, inet));
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

            return cass_error_to_nif_term(env, fun.set_uuid(obj, index, uuid));
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

            return cass_error_to_nif_term(env, fun.set_decimal(obj, index, varint.data, varint.size, scale));
        }

        case CASS_VALUE_TYPE_MAP:
        case CASS_VALUE_TYPE_LIST:
        case CASS_VALUE_TYPE_SET:
        {
            CassCollection* collection = NULL;

            ERL_NIF_TERM result = nif_list_to_cass_collection(env, value, data_type, &collection);

            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;

            CassError error = fun.set_collection(obj, index, collection);
            cass_collection_free(collection);
            return cass_error_to_nif_term(env, error);
        }

        case CASS_VALUE_TYPE_TUPLE:
        {
            CassTuple* nested_tuple = NULL;

            ERL_NIF_TERM result = nif_term_to_cass_tuple(env, value, data_type, &nested_tuple);

            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;

            CassError error = fun.set_tuple(obj, index, nested_tuple);
            cass_tuple_free(nested_tuple);
            return cass_error_to_nif_term(env, error);
        }

        case CASS_VALUE_TYPE_UDT:
        {
            CassUserType* nested_udt = NULL;

            ERL_NIF_TERM result = nif_term_to_cass_udt(env, value, data_type, &nested_udt);

            if(!enif_is_identical(result, ATOMS.atomOk))
                return result;

            CassError error = fun.set_user_type(obj, index, nested_udt);
            cass_user_type_free(nested_udt);
            return cass_error_to_nif_term(env, error);
        }

        default:
            return make_error(env, erlcass::kFailedToSetUnknownType);
    }
}

ERL_NIF_TERM nif_term_to_cass_udt(ErlNifEnv* env, ERL_NIF_TERM term, const datastax::internal::core::DataType* dt, CassUserType** udt)
{
    unsigned int length;

    if(!enif_get_list_length(env, term, &length) || length < 1)
        return make_badarg(env);

    scoped_ptr(data_type, CassDataType, cass_data_type_new_udt(length), cass_data_type_free);

    const datastax::internal::core::UserType* ut = static_cast<const datastax::internal::core::UserType*>(dt);

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

        datastax::internal::core::IndexVec indices;

        if(ut->get_indices(datastax::StringRef(BIN_TO_STR(bin.data), bin.size), &indices) == 0)
            return make_badarg(env);

        size_t index = indices[0];
        const datastax::internal::core::DataType* type = ut->fields().at(index).type.get();

        ERL_NIF_TERM item_term = cass_set_from_nif(env, utv.get(), index, kCassUserTypeFuns, type, items[1]);

        if(!enif_is_identical(item_term, ATOMS.atomOk))
            return item_term;
    }

    *udt = utv.release();
    return ATOMS.atomOk;
}

ERL_NIF_TERM nif_term_to_cass_tuple(ErlNifEnv* env, ERL_NIF_TERM term, const datastax::internal::core::DataType* data_type, CassTuple** tp)
{
    const datastax::internal::core::CompositeType* ct = static_cast<const datastax::internal::core::CompositeType*>(data_type);

    const ERL_NIF_TERM *items;
    int arity;

    if(!enif_get_tuple(env, term, &arity, &items) || arity == 0 || static_cast<size_t>(arity) != ct->types().size())
        return make_badarg(env);

    scoped_ptr(tuple, CassTuple, cass_tuple_new(arity), cass_tuple_free);
    size_t index = 0;

    for(datastax::internal::core::DataType::Vec::const_iterator it = ct->types().begin(); it != ct->types().end(); ++it)
    {
        ERL_NIF_TERM item_term = cass_set_from_nif(env, tuple.get(), index, kCassTupleFuns, (*it).get(), items[index]);

        if(!enif_is_identical(item_term, ATOMS.atomOk))
            return item_term;

        index++;
    }

    *tp = tuple.release();
    return ATOMS.atomOk;
}

// collections

ERL_NIF_TERM populate_list_set_collection(ErlNifEnv* env, ERL_NIF_TERM list, CassCollection* collection, const datastax::internal::core::DataType* dt)
{
    ERL_NIF_TERM head;

    while(enif_get_list_cell(env, list, &head, &list))
    {
        ERL_NIF_TERM item_term = cass_set_from_nif(env, collection, 0, kCassCollectionFuns, dt, head);

        if(!enif_is_identical(item_term, ATOMS.atomOk))
            return item_term;
    }

    return ATOMS.atomOk;
}

ERL_NIF_TERM populate_map_collection(ErlNifEnv* env, ERL_NIF_TERM list, CassCollection* collection, const datastax::internal::core::DataType* kt, const datastax::internal::core::DataType* vt)
{
    ERL_NIF_TERM head;
    const ERL_NIF_TERM *items;
    int arity;

    while(enif_get_list_cell(env, list, &head, &list))
    {
        if(!enif_get_tuple(env, head, &arity, &items) || arity != 2)
            return make_badarg(env);

        // add key
        ERL_NIF_TERM item_term = cass_set_from_nif(env, collection, 0, kCassCollectionFuns, kt, items[0]);

        if(!enif_is_identical(item_term, ATOMS.atomOk))
            return item_term;

        // add value

        item_term = cass_set_from_nif(env, collection, 0, kCassCollectionFuns, vt, items[1]);

        if(!enif_is_identical(item_term, ATOMS.atomOk))
            return item_term;
    }

    return ATOMS.atomOk;
}

ERL_NIF_TERM nif_list_to_cass_collection(ErlNifEnv* env, ERL_NIF_TERM list, const datastax::internal::core::DataType* data_type, CassCollection ** col)
{
    const datastax::internal::core::CompositeType* ct = static_cast<const datastax::internal::core::CompositeType*>(data_type);

    unsigned int length;

    if(!enif_get_list_length(env, list, &length))
        return make_badarg(env);

    CassCollectionType type = static_cast<CassCollectionType>(data_type->value_type());
    scoped_ptr(collection, CassCollection, cass_collection_new(type, length), cass_collection_free);

    ERL_NIF_TERM return_value;

    if(data_type->value_type() != CASS_VALUE_TYPE_MAP)
        return_value = populate_list_set_collection(env, list, collection.get(), ct->types().at(kIndexKey).get());
    else
        return_value = populate_map_collection(env, list, collection.get(), ct->types().at(kIndexKey).get(), ct->types().at(kIndexVal).get());

    if(!enif_is_identical(return_value, ATOMS.atomOk))
        return return_value;

    *col = collection.release();
    return ATOMS.atomOk;
}

ERL_NIF_TERM cass_bind_by_index(ErlNifEnv* env, CassStatement* statement, size_t index, const datastax::internal::core::DataType* data_type, ERL_NIF_TERM value)
{
    if(enif_is_identical(value, ATOMS.atomNull))
        return cass_error_to_nif_term(env, cass_statement_bind_null(statement, index));

    return cass_set_from_nif(env, statement, index, kCassStatementFuns, data_type, value);
}

