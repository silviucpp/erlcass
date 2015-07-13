//
//  metadata.h
//  erlcass
//
//  Created by silviu on 5/13/15.
//
//

#ifndef __erlcass__metadata__
#define __erlcass__metadata__

#include "erl_nif.h"
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

SchemaColumn atom_to_schema_column(ErlNifEnv* env, ERL_NIF_TERM value);

#endif /* defined(__erlcass__metadata__) */
