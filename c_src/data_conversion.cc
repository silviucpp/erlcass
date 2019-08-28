#include "data_conversion.h"
#include "erlcass.h"
#include "nif_utils.h"
#include "uuid_serialization.h"
#include "macros.h"

#include <memory>
#include <string.h>
#include <vector>

static const cass_uint32_t kEpochDaysOffset = 2147483648;

ERL_NIF_TERM string_to_erlang_term(ErlNifEnv* env, const CassValue* value);
ERL_NIF_TERM blob_to_erlang_term(ErlNifEnv* env, const CassValue* value);
ERL_NIF_TERM uuid_to_erlang_term(ErlNifEnv* env, const CassValue* value);
ERL_NIF_TERM inet_to_erlang_term(ErlNifEnv* env, const CassValue* value);
ERL_NIF_TERM decimal_to_erlang_term(ErlNifEnv* env, const CassValue* value);
ERL_NIF_TERM date_to_erlang_term(ErlNifEnv* env, const CassValue* value);
ERL_NIF_TERM int64_to_erlang_term(ErlNifEnv* env, const CassValue* value);
ERL_NIF_TERM bool_to_erlang_term(const CassValue* value);
ERL_NIF_TERM float_to_erlang_term(ErlNifEnv* env, const CassValue* value);
ERL_NIF_TERM double_to_erlang_term(ErlNifEnv* env, const CassValue* value);
ERL_NIF_TERM tiny_int_to_erlang_term(ErlNifEnv* env, const CassValue* value);
ERL_NIF_TERM small_int_to_erlang_term(ErlNifEnv* env, const CassValue* value);
ERL_NIF_TERM int_to_erlang_term(ErlNifEnv* env, const CassValue* value);
ERL_NIF_TERM collection_to_erlang_term(ErlNifEnv* env, const CassValue* value);
ERL_NIF_TERM tuple_to_erlang_term(ErlNifEnv* env, const CassValue* value);
ERL_NIF_TERM udt_to_erlang_term(ErlNifEnv* env, const CassValue* value);

ERL_NIF_TERM cass_value_to_nif_term(ErlNifEnv* env, const CassValue* value)
{
    if(cass_value_is_null(value))
        return ATOMS.atomNull;

    switch (cass_value_type(value))
    {
        case CASS_VALUE_TYPE_ASCII:
        case CASS_VALUE_TYPE_TEXT:
        case CASS_VALUE_TYPE_VARCHAR:
            return string_to_erlang_term(env, value);

        case CASS_VALUE_TYPE_TIME:
        case CASS_VALUE_TYPE_TIMESTAMP:
        case CASS_VALUE_TYPE_COUNTER:
        case CASS_VALUE_TYPE_BIGINT:
            return int64_to_erlang_term(env, value);

        case CASS_VALUE_TYPE_BOOLEAN:
            return bool_to_erlang_term(value);

        case CASS_VALUE_TYPE_FLOAT:
            return float_to_erlang_term(env, value);

        case CASS_VALUE_TYPE_DOUBLE:
            return double_to_erlang_term(env, value);

        case CASS_VALUE_TYPE_TINY_INT:
            return tiny_int_to_erlang_term(env, value);

        case CASS_VALUE_TYPE_SMALL_INT:
            return small_int_to_erlang_term(env, value);

        case CASS_VALUE_TYPE_INT:
            return int_to_erlang_term(env, value);

        case CASS_VALUE_TYPE_DATE:
            return date_to_erlang_term(env, value);

        case CASS_VALUE_TYPE_VARINT:
        case CASS_VALUE_TYPE_BLOB:
            return blob_to_erlang_term(env, value);

        case CASS_VALUE_TYPE_TIMEUUID:
        case CASS_VALUE_TYPE_UUID:
            return uuid_to_erlang_term(env, value);

        case CASS_VALUE_TYPE_INET:
            return inet_to_erlang_term(env, value);

        case CASS_VALUE_TYPE_DECIMAL:
            return decimal_to_erlang_term(env, value);

        case CASS_VALUE_TYPE_SET:
        case CASS_VALUE_TYPE_LIST:
        case CASS_VALUE_TYPE_MAP:
            return collection_to_erlang_term(env, value);

        case CASS_VALUE_TYPE_TUPLE:
            return tuple_to_erlang_term(env, value);

        case CASS_VALUE_TYPE_UDT:
            return udt_to_erlang_term(env, value);

        default:
            // unsuported types and null values
            return ATOMS.atomNull;
    }
}

ERL_NIF_TERM cass_data_type_to_nif_term(ErlNifEnv* env, const CassDataType* data_type)
{
    CassValueType type = cass_data_type_type(data_type);

    switch(type)
    {
        case CASS_VALUE_TYPE_TEXT:
            return ATOMS.atomText;

        case CASS_VALUE_TYPE_ASCII:
            return ATOMS.atomAscii;

        case CASS_VALUE_TYPE_VARCHAR:
            return ATOMS.atomText;

        case CASS_VALUE_TYPE_TIMESTAMP:
            return ATOMS.atomTimestamp;

        case CASS_VALUE_TYPE_TIME:
            return ATOMS.atomTime;

        case CASS_VALUE_TYPE_TINY_INT:
            return ATOMS.atomTinyInt;

        case CASS_VALUE_TYPE_SMALL_INT:
            return ATOMS.atomSmallInt;

        case CASS_VALUE_TYPE_VARINT:
            return ATOMS.atomVarint;

        case CASS_VALUE_TYPE_INT:
            return ATOMS.atomInt;

        case CASS_VALUE_TYPE_BIGINT:
            return ATOMS.atomBigInt;

        case CASS_VALUE_TYPE_DATE:
            return ATOMS.atomDate;

        case CASS_VALUE_TYPE_BLOB:
            return ATOMS.atomBlob;

        case CASS_VALUE_TYPE_BOOLEAN:
            return ATOMS.atomBool;

        case CASS_VALUE_TYPE_FLOAT:
            return ATOMS.atomFloat;

        case CASS_VALUE_TYPE_DOUBLE:
            return ATOMS.atomDouble;

        case CASS_VALUE_TYPE_INET:
            return ATOMS.atomInet;

        case CASS_VALUE_TYPE_TIMEUUID:
            return ATOMS.atomTimeUuid;

        case CASS_VALUE_TYPE_UUID:
            return ATOMS.atomUuid;

        case CASS_VALUE_TYPE_DECIMAL:
            return ATOMS.atomDecimal;

        case CASS_VALUE_TYPE_LIST:
        {
            const CassDataType* value_dt = cass_data_type_sub_data_type(data_type, 0);
            return enif_make_tuple2(env, ATOMS.atomList, cass_data_type_to_nif_term(env, value_dt));
        }

        case CASS_VALUE_TYPE_MAP:
        {
            const CassDataType* key_dt = cass_data_type_sub_data_type(data_type, 0);
            const CassDataType* value_dt = cass_data_type_sub_data_type(data_type, 1);
            return enif_make_tuple3(env, ATOMS.atomMap, cass_data_type_to_nif_term(env, key_dt), cass_data_type_to_nif_term(env, value_dt));
        }

        case CASS_VALUE_TYPE_SET:
        {
            const CassDataType* value_dt = cass_data_type_sub_data_type(data_type, 0);
            return enif_make_tuple2(env, ATOMS.atomSet, cass_data_type_to_nif_term(env, value_dt));
        }

        case CASS_VALUE_TYPE_TUPLE:
        {
            size_t length = cass_data_type_sub_type_count(data_type);

            std::vector<ERL_NIF_TERM> columns_dt;

            for(size_t i = 0; i < length; i++)
            {
                const CassDataType* value_dt = cass_data_type_sub_data_type(data_type, i);
                columns_dt.push_back(cass_data_type_to_nif_term(env, value_dt));
            }

            return enif_make_tuple2(env, ATOMS.atomTuple, enif_make_list_from_array(env, columns_dt.data(), columns_dt.size()));
        }

        case CASS_VALUE_TYPE_UDT:
        {
            const char* buff_ptr;
            size_t buff_size;

            if(cass_data_type_type_name(data_type, &buff_ptr, &buff_size) != CASS_OK)
                return ATOMS.atomNull;

            size_t length = cass_data_type_sub_type_count(data_type);
            std::vector<ERL_NIF_TERM> columns_dt;

            for(size_t i = 0; i < length; i++)
            {
                const char* sb_name;
                size_t sb_name_size;
                cass_data_type_sub_type_name(data_type, i, &sb_name, &sb_name_size);

                const CassDataType* value_dt = cass_data_type_sub_data_type(data_type, i);
                columns_dt.push_back(enif_make_tuple2(env, make_binary(env, sb_name, sb_name_size), cass_data_type_to_nif_term(env, value_dt)));
            }

            return enif_make_tuple3(env, ATOMS.atomUdt, make_binary(env, buff_ptr, buff_size), enif_make_list_from_array(env, columns_dt.data(), columns_dt.size()));
        }

        default:
            return ATOMS.atomNull;
    }
}

ERL_NIF_TERM string_to_erlang_term(ErlNifEnv* env, const CassValue* value)
{
    const char* buff_ptr;
    size_t buff_size;
    cass_value_get_string(value, &buff_ptr, &buff_size);
    return make_binary(env, buff_ptr, buff_size);
}

ERL_NIF_TERM blob_to_erlang_term(ErlNifEnv* env, const CassValue* value)
{
    const cass_byte_t * buffer = NULL;
    size_t buffer_size;
    cass_value_get_bytes(value, &buffer, &buffer_size);
    return make_binary(env, (const char*)buffer, buffer_size);
}

ERL_NIF_TERM uuid_to_erlang_term(ErlNifEnv* env, const CassValue* value)
{
    CassUuid uuid;
    cass_value_get_uuid(value, &uuid);

    char uuid_str[CASS_UUID_STRING_LENGTH];
    erlcass::cass_uuid_string(uuid, uuid_str);

    return make_binary(env, uuid_str, CASS_UUID_STRING_LENGTH - 1);
}

ERL_NIF_TERM inet_to_erlang_term(ErlNifEnv* env, const CassValue* value)
{
    CassInet inet;
    cass_value_get_inet(value, &inet);

    char inet_str[CASS_INET_STRING_LENGTH];
    cass_inet_string(inet, inet_str);

    return make_binary(env, inet_str, strlen(inet_str));
}

ERL_NIF_TERM decimal_to_erlang_term(ErlNifEnv* env, const CassValue* value)
{
    const cass_byte_t * buffer = NULL;
    size_t buffer_size;
    cass_int32_t scale;
    cass_value_get_decimal(value, &buffer, &buffer_size, &scale);

    return enif_make_tuple2(env, make_binary(env, reinterpret_cast<const char*>(buffer), buffer_size), enif_make_int(env, scale));
}

ERL_NIF_TERM int64_to_erlang_term(ErlNifEnv* env, const CassValue* value)
{
    cass_int64_t value_long;
    cass_value_get_int64(value, &value_long);
    return enif_make_int64(env, value_long);
}

ERL_NIF_TERM bool_to_erlang_term(const CassValue* value)
{
    cass_bool_t bool_val;
    cass_value_get_bool(value, &bool_val);
    return bool_val ? ATOMS.atomTrue : ATOMS.atomFalse;
}

ERL_NIF_TERM float_to_erlang_term(ErlNifEnv* env, const CassValue* value)
{
    cass_float_t value_float;
    cass_value_get_float(value, &value_float);
    return enif_make_double(env, value_float);
}

ERL_NIF_TERM double_to_erlang_term(ErlNifEnv* env, const CassValue* value)
{
    cass_double_t value_double;
    cass_value_get_double(value, &value_double);
    return enif_make_double(env, value_double);
}

ERL_NIF_TERM date_to_erlang_term(ErlNifEnv* env, const CassValue* value)
{
    cass_uint32_t value_uint;
    cass_value_get_uint32(value, &value_uint);
    // Values of the `date` type are encoded as 32-bit unsigned integers
    // representing a number of days with epoch (January 1st, 1970) at the center of the
    // range (2^31).
    value_uint -= kEpochDaysOffset;
    return enif_make_uint(env, value_uint);
}

ERL_NIF_TERM int_to_erlang_term(ErlNifEnv* env, const CassValue* value)
{
    cass_int32_t value_int;
    cass_value_get_int32(value, &value_int);
    return enif_make_int(env, value_int);
}

ERL_NIF_TERM tiny_int_to_erlang_term(ErlNifEnv* env, const CassValue* value)
{
    cass_int8_t value_int;
    cass_value_get_int8(value, &value_int);
    return enif_make_int(env, value_int);
}

ERL_NIF_TERM small_int_to_erlang_term(ErlNifEnv* env, const CassValue* value)
{
    cass_int16_t value_int;
    cass_value_get_int16(value, &value_int);
    return enif_make_int(env, value_int);
}

ERL_NIF_TERM collection_to_erlang_term(ErlNifEnv* env, const CassValue* value)
{
    size_t items_count = cass_value_item_count(value);

    if(items_count == 0)
        return enif_make_list(env, 0);

    std::vector<ERL_NIF_TERM> array;
    array.reserve(items_count);

    if(cass_value_type(value) == CASS_VALUE_TYPE_MAP)
    {
        scoped_ptr(iterator, CassIterator, cass_iterator_from_map(value), cass_iterator_free);

        while (cass_iterator_next(iterator.get()))
        {
            const CassValue* c_key = cass_iterator_get_map_key(iterator.get());
            const CassValue* c_value = cass_iterator_get_map_value(iterator.get());
            array.push_back(enif_make_tuple2(env, cass_value_to_nif_term(env, c_key), cass_value_to_nif_term(env, c_value)));
        }
    }
    else
    {
        scoped_ptr(iterator, CassIterator, cass_iterator_from_collection(value), cass_iterator_free);

        while (cass_iterator_next(iterator.get()))
            array.push_back(cass_value_to_nif_term(env, cass_iterator_get_value(iterator.get())));
    }

    return enif_make_list_from_array(env, array.data(), array.size());
}

ERL_NIF_TERM tuple_to_erlang_term(ErlNifEnv* env, const CassValue* value)
{
    size_t items_count = cass_value_item_count(value);

    if(items_count == 0)
        return enif_make_tuple(env, 0);

    std::vector<ERL_NIF_TERM> items;
    items.resize(items_count);

    scoped_ptr(iterator, CassIterator, cass_iterator_from_tuple(value), cass_iterator_free);

    while (cass_iterator_next(iterator.get()))
        items.push_back(cass_value_to_nif_term(env, cass_iterator_get_value(iterator.get())));

    return enif_make_tuple_from_array(env, items.data(), items.size());
}

ERL_NIF_TERM udt_to_erlang_term(ErlNifEnv* env, const CassValue* value)
{
    size_t items_count = cass_value_item_count(value);

    if(items_count == 0)
        return ATOMS.atomNull;

    std::vector<ERL_NIF_TERM> array;
    array.reserve(items_count);

    scoped_ptr(iterator, CassIterator, cass_iterator_fields_from_user_type(value), cass_iterator_free);

    while (cass_iterator_next(iterator.get()))
    {
        const char* field_name_ptr;
        size_t field_name_length;

        if(cass_iterator_get_user_type_field_name(iterator.get(), &field_name_ptr, &field_name_length) != CASS_OK)
            return ATOMS.atomNull;

        ERL_NIF_TERM field_value = cass_value_to_nif_term(env, cass_iterator_get_user_type_field_value(iterator.get()));
        array.push_back(enif_make_tuple2(env, make_binary(env, field_name_ptr, field_name_length), field_value));
    }

    return enif_make_list_from_array(env, array.data(), array.size());
}

ERL_NIF_TERM column_data_to_erlang_term(ErlNifEnv* env, const CassResult* result, size_t columns_count)
{
    std::vector<ERL_NIF_TERM> array;
    array.reserve(columns_count);

    for(size_t index = 0; index < columns_count; index++)
    {
        const char* name;
        size_t name_length;

        if (cass_result_column_name(result, index, &name, &name_length) != CASS_OK)
            return enif_make_list(env, 0);

        const CassDataType *column_dt = cass_result_column_data_type (result, index);
        ERL_NIF_TERM dt_type_term = cass_data_type_to_nif_term(env, column_dt);
        array.push_back(enif_make_tuple2(env, make_binary(env, name, name_length), dt_type_term));
    }

    return enif_make_list_from_array(env, array.data(), array.size());
}

ERL_NIF_TERM cass_result_to_erlang_term(ErlNifEnv* env, const CassResult* result)
{
    size_t columns_count = cass_result_column_count(result);

    if(columns_count == 0)
        return ATOMS.atomOk;

    ERL_NIF_TERM column_data = column_data_to_erlang_term(env, result, columns_count);
    size_t rows_count = cass_result_row_count(result);

    if(rows_count == 0)
        return enif_make_tuple3(env, ATOMS.atomOk, column_data, enif_make_list(env, 0));

    std::vector<ERL_NIF_TERM> array_columns;
    std::vector<ERL_NIF_TERM> array_rows;
    array_columns.reserve(columns_count);
    array_rows.reserve(rows_count);

    scoped_ptr(iterator, CassIterator, cass_iterator_from_result(result), cass_iterator_free);

    while (cass_iterator_next(iterator.get()))
    {
        const CassRow* row = cass_iterator_get_row(iterator.get());

        for(size_t i = 0; i < columns_count; i++)
            array_columns[i] = cass_value_to_nif_term(env, cass_row_get_column(row, i));

        array_rows.push_back(enif_make_list_from_array(env, array_columns.data(), columns_count));
    }

    return enif_make_tuple3(env, ATOMS.atomOk, column_data, enif_make_list_from_array(env, array_rows.data(), array_rows.size()));
}

ERL_NIF_TERM get_column_meta(ErlNifEnv* env, const CassColumnMeta* meta)
{
    ERL_NIF_TERM colum_data[3];

    const char* name;
    size_t name_length;

    cass_column_meta_name(meta, &name, &name_length);
    colum_data[0] = enif_make_tuple2(env, ATOMS.atomColumnMetaColumnName, make_binary(env, name, name_length));

    ERL_NIF_TERM column_data_type = cass_data_type_to_nif_term(env, cass_column_meta_data_type(meta));
    colum_data[1] = enif_make_tuple2(env, ATOMS.atomColumnMetaDataType, column_data_type);

    ERL_NIF_TERM column_type;

    switch(cass_column_meta_type(meta))
    {
        case CASS_COLUMN_TYPE_REGULAR:
            column_type = ATOMS.atomColumnTypeRegular;
            break;

        case CASS_COLUMN_TYPE_PARTITION_KEY:
            column_type = ATOMS.atomColumnTypePartitionKey;
            break;

        case CASS_COLUMN_TYPE_CLUSTERING_KEY:
            column_type = ATOMS.atomColumnTypeClusteringKey;
            break;

        case CASS_COLUMN_TYPE_STATIC:
            column_type = ATOMS.atomColumnTypeStatic;
            break;

        case CASS_COLUMN_TYPE_COMPACT_VALUE:
            column_type = ATOMS.atomColumnTypeCompactValue;
            break;

        default:
            column_type = ATOMS.atomNull;
    }

    colum_data[2] = enif_make_tuple2(env, ATOMS.atomColumnMetaType, column_type);
    return enif_make_list_from_array(env, colum_data, 3);
}

ERL_NIF_TERM get_meta_field(ErlNifEnv* env, const CassIterator* iterator)
{
    const char* name;
    size_t name_length;

    if(cass_iterator_get_meta_field_name(iterator, &name, &name_length) != CASS_OK)
        return ATOMS.atomNull;

    const CassValue* value = cass_iterator_get_meta_field_value(iterator);
    return enif_make_tuple2(env, make_binary(env, name, name_length), cass_value_to_nif_term(env, value));
}

ERL_NIF_TERM cass_table_meta_fields_to_erlang_term(ErlNifEnv* env, const CassTableMeta* meta)
{
    const char* name;
    size_t name_length;

    cass_table_meta_name(meta, &name, &name_length);
    ERL_NIF_TERM table_name = make_binary(env, name, name_length);

    scoped_ptr(field_it, CassIterator, cass_iterator_fields_from_table_meta(meta), cass_iterator_free);

    std::vector<ERL_NIF_TERM> field_data;

    while (cass_iterator_next(field_it.get()))
        field_data.push_back(get_meta_field(env, field_it.get()));

    ERL_NIF_TERM table_field_data = enif_make_list_from_array(env, field_data.data(), field_data.size());

    std::vector<ERL_NIF_TERM> column_data;
    column_data.reserve(cass_table_meta_column_count(meta));

    scoped_ptr(iterator, CassIterator, cass_iterator_columns_from_table_meta(meta), cass_iterator_free);

    while (cass_iterator_next(iterator.get()))
        column_data.push_back(get_column_meta(env, cass_iterator_get_column_meta(iterator.get())));

    ERL_NIF_TERM column_field_data = enif_make_list_from_array(env, column_data.data(), column_data.size());
    return enif_make_tuple3(env, table_name, table_field_data, column_field_data);
}

ERL_NIF_TERM cass_keyspace_meta_fields_to_erlang_term(ErlNifEnv* env, const CassKeyspaceMeta* keyspace_meta)
{
    const char* name;
    size_t name_length;

    cass_keyspace_meta_name(keyspace_meta, &name, &name_length);
    ERL_NIF_TERM keyspace_name = make_binary(env, name, name_length);

    std::vector<ERL_NIF_TERM> field_data;

    scoped_ptr(field_it, CassIterator, cass_iterator_fields_from_keyspace_meta(keyspace_meta), cass_iterator_free);

    while (cass_iterator_next(field_it.get()))
        field_data.push_back(get_meta_field(env, field_it.get()));

    ERL_NIF_TERM keyspace_field_data = enif_make_list_from_array(env, field_data.data(), field_data.size());

    scoped_ptr(tables_it, CassIterator, cass_iterator_tables_from_keyspace_meta(keyspace_meta), cass_iterator_free);
    std::vector<ERL_NIF_TERM> tables_data_vec;

    while (cass_iterator_next(tables_it.get()))
        tables_data_vec.push_back(cass_table_meta_fields_to_erlang_term(env, cass_iterator_get_table_meta(tables_it.get())));

    ERL_NIF_TERM tables_data = enif_make_list_from_array(env, tables_data_vec.data(), tables_data_vec.size());

    scoped_ptr(user_types_it, CassIterator, cass_iterator_user_types_from_keyspace_meta(keyspace_meta), cass_iterator_free);
    std::vector<ERL_NIF_TERM> user_types_data_vec;

    while (cass_iterator_next(user_types_it.get()))
        user_types_data_vec.push_back(cass_data_type_to_nif_term(env, cass_iterator_get_user_type(user_types_it.get())));

    ERL_NIF_TERM user_types_data = enif_make_list_from_array(env, user_types_data_vec.data(), user_types_data_vec.size());
    return enif_make_tuple4(env, keyspace_name, keyspace_field_data, tables_data, user_types_data);
}

ERL_NIF_TERM cass_schema_meta_fields_to_erlang_term(ErlNifEnv* env, const CassSchemaMeta* schema_meta)
{
    CassVersion version = cass_schema_meta_version(schema_meta);
    ERL_NIF_TERM version_data = enif_make_tuple3(env,
                                                 enif_make_int(env, version.major_version),
                                                 enif_make_int(env, version.minor_version),
                                                 enif_make_int(env, version.patch_version));

    std::vector<ERL_NIF_TERM> keyspaces_data_vec;
    scoped_ptr(keyspace_it, CassIterator, cass_iterator_keyspaces_from_schema_meta(schema_meta), cass_iterator_free);

    while (cass_iterator_next(keyspace_it.get()))
        keyspaces_data_vec.push_back(cass_keyspace_meta_fields_to_erlang_term(env, cass_iterator_get_keyspace_meta(keyspace_it.get())));

    ERL_NIF_TERM keyspaces_data = enif_make_list_from_array(env, keyspaces_data_vec.data(), keyspaces_data_vec.size());
    return enif_make_tuple2(env, enif_make_tuple2(env, ATOMS.atomMetadataSchemaVersion, version_data), keyspaces_data);
}
