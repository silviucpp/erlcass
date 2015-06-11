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

struct SchemaColumn;

SchemaColumn atom_to_schema_column(ErlNifEnv* env, ERL_NIF_TERM value);

#endif /* defined(__erlcass__metadata__) */
