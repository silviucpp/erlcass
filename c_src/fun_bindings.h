#ifndef C_SRC_FUN_BINDINGS_H_
#define C_SRC_FUN_BINDINGS_H_

#include "cassandra.h"

CassError erlcass_cass_collection_append_string_n(CassCollection* collection, size_t index, const char* value, size_t value_length);
CassError erlcass_cass_collection_append_int8(CassCollection* collection, size_t index, cass_int8_t value);
CassError erlcass_cass_collection_append_int16(CassCollection* collection, size_t index, cass_int16_t value);
CassError erlcass_cass_collection_append_int32(CassCollection* collection, size_t index, cass_int32_t value);
CassError erlcass_cass_collection_append_int64(CassCollection* collection, size_t index, cass_int64_t value);
CassError erlcass_cass_collection_append_uint32(CassCollection* collection, size_t index, cass_uint32_t value);
CassError erlcass_cass_collection_append_bytes(CassCollection* collection, size_t index, const cass_byte_t* value, size_t value_size);
CassError erlcass_cass_collection_append_bool(CassCollection* collection, size_t index, cass_bool_t value);
CassError erlcass_cass_collection_append_float(CassCollection* collection, size_t index, cass_float_t value);
CassError erlcass_cass_collection_append_double(CassCollection* collection, size_t index, cass_double_t value);
CassError erlcass_cass_collection_append_inet(CassCollection* collection, size_t index, const CassInet& value);
CassError erlcass_cass_collection_append_uuid(CassCollection* collection, size_t index, CassUuid value);
CassError erlcass_cass_collection_append_decimal(CassCollection* collection, size_t index, const cass_byte_t* varint, size_t varint_size, cass_int32_t scale);
CassError erlcass_cass_collection_append_collection(CassCollection* collection, size_t index, const CassCollection* value);
CassError erlcass_cass_collection_append_tuple(CassCollection* collection, size_t index, const CassTuple* value);
CassError erlcass_cass_collection_append_user_type(CassCollection* collection, size_t index, const CassUserType* value);

#endif  // C_SRC_FUN_BINDINGS_H_
