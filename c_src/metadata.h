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
#include "schema.h"

SchemaColumn atom_to_cass_value_type(ErlNifEnv* env, ERL_NIF_TERM value);
bool parse_statement_metadata(ErlNifEnv* env, ERL_NIF_TERM list, ColumnsMap* metadata);

#endif /* defined(__erlcass__metadata__) */
