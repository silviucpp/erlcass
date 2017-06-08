#ifndef ERLCASS_C_SRC_UUID_SERIALIZATION_H
#define ERLCASS_C_SRC_UUID_SERIALIZATION_H

#include "cassandra.h"

namespace erlcass {

void cass_uuid_string(CassUuid uuid, char* output);
CassError cass_uuid_from_string_n(const char* str, size_t str_length, CassUuid* output);

}

#endif
