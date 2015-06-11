//
//  uuid_serialization.h
//  erlcass
//
//  Created by silviu on 6/11/15.
//
//

#ifndef __erlcass__uuid_serialization__
#define __erlcass__uuid_serialization__

#include "cassandra.h"

namespace erlcass {
    
void cass_uuid_string(CassUuid uuid, char* output);
CassError cass_uuid_from_string_n(const char* str, size_t str_length, CassUuid* output);
    
}

#endif /* defined(__erlcass__uuid_serialization__) */
