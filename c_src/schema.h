//
//  schema.h
//  erlcass
//
//  Created by silviu on 6/5/15.
//
//

#ifndef __erlcass__schema__
#define __erlcass__schema__

#include "cassandra.h"

#include <string>
#include <map>

struct SchemaColumn
{
    SchemaColumn() : type(CASS_VALUE_TYPE_UNKNOWN), keyType(CASS_VALUE_TYPE_UNKNOWN), valueType(CASS_VALUE_TYPE_UNKNOWN) {}
    SchemaColumn(CassValueType vt) : type(vt), keyType(CASS_VALUE_TYPE_UNKNOWN), valueType(vt) {}
    SchemaColumn(CassValueType tp, CassValueType vt) : type(tp), keyType(CASS_VALUE_TYPE_UNKNOWN), valueType(vt) {}
    SchemaColumn(CassValueType tp, CassValueType kt, CassValueType vt) : type(tp), keyType(kt), valueType(vt) {}
    
    CassValueType type;
    CassValueType keyType;
    CassValueType valueType;
};

typedef std::map<std::string, SchemaColumn> ColumnsMap;

bool get_table_schema(CassSession* session, const std::string& keyspace, const std::string& table, ColumnsMap* schema_map);

#endif /* defined(__erlcass__schema__) */
