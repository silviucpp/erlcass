#include "fun_bindings.h"
#include "macros.h"

CassError erlcass_cass_collection_append_string_n(CassCollection* collection, size_t index, const char* value, size_t value_length)
{
    UNUSED(index);
    return cass_collection_append_string_n(collection, value, value_length);
}

CassError erlcass_cass_collection_append_int8(CassCollection* collection, size_t index, cass_int8_t value)
{
    UNUSED(index);
    return cass_collection_append_int8(collection, value);
}

CassError erlcass_cass_collection_append_int16(CassCollection* collection, size_t index, cass_int16_t value)
{
    UNUSED(index);
    return cass_collection_append_int16(collection, value);
}

CassError erlcass_cass_collection_append_int32(CassCollection* collection, size_t index, cass_int32_t value)
{
    UNUSED(index);
    return cass_collection_append_int32(collection, value);
}

CassError erlcass_cass_collection_append_int64(CassCollection* collection, size_t index, cass_int64_t value)
{
    UNUSED(index);
    return cass_collection_append_int64(collection, value);
}

CassError erlcass_cass_collection_append_uint32(CassCollection* collection, size_t index, cass_uint32_t value)
{
    UNUSED(index);
    return cass_collection_append_uint32(collection, value);
}

CassError erlcass_cass_collection_append_bytes(CassCollection* collection, size_t index, const cass_byte_t* value, size_t value_size)
{
    UNUSED(index);
    return cass_collection_append_bytes(collection, value, value_size);
}

CassError erlcass_cass_collection_append_bool(CassCollection* collection, size_t index, cass_bool_t value)
{
    UNUSED(index);
    return cass_collection_append_bool(collection, value);
}

CassError erlcass_cass_collection_append_float(CassCollection* collection, size_t index, cass_float_t value)
{
    UNUSED(index);
    return cass_collection_append_float(collection, value);
}

CassError erlcass_cass_collection_append_double(CassCollection* collection, size_t index, cass_double_t value)
{
    UNUSED(index);
    return cass_collection_append_double(collection, value);
}

CassError erlcass_cass_collection_append_inet(CassCollection* collection, size_t index, const CassInet& value)
{
    UNUSED(index);
    return cass_collection_append_inet(collection, value);
}

CassError erlcass_cass_collection_append_uuid(CassCollection* collection, size_t index, CassUuid value)
{
    UNUSED(index);
    return cass_collection_append_uuid(collection, value);
}

CassError erlcass_cass_collection_append_decimal(CassCollection* collection, size_t index, const cass_byte_t* varint, size_t varint_size, cass_int32_t scale)
{
    UNUSED(index);
    return cass_collection_append_decimal(collection, varint, varint_size, scale);
}

CassError erlcass_cass_collection_append_collection(CassCollection* collection, size_t index, const CassCollection* value)
{
    UNUSED(index);
    return cass_collection_append_collection(collection, value);
}

CassError erlcass_cass_collection_append_tuple(CassCollection* collection, size_t index, const CassTuple* value)
{
    UNUSED(index);
    return cass_collection_append_tuple(collection, value);
}

CassError erlcass_cass_collection_append_user_type(CassCollection* collection, size_t index, const CassUserType* value)
{
    UNUSED(index);
    return cass_collection_append_user_type(collection, value);
}
