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

#include <vector>

#define KEY_INDEX 0
#define VAL_INDEX 1

struct SchemaColumn
{
    SchemaColumn() : type(CASS_VALUE_TYPE_UNKNOWN){}
    explicit SchemaColumn(CassValueType vt) : type(vt){}
    
    CassValueType type;
    std::vector<SchemaColumn> subtypes;
};

SchemaColumn atom_to_schema_column(ErlNifEnv* env, ERL_NIF_TERM value);

#endif /* defined(__erlcass__metadata__) */
