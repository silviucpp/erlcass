//
//  metadata.h
//  erlcass
//
//  Created by silviu on 5/13/15.
//
//

#ifndef __erlcass__metadata__
#define __erlcass__metadata__

#include "erlcass.h"

ItemType atom_to_cass_value_type(ErlNifEnv* env, ERL_NIF_TERM value);
bool parse_statement_metadata(ErlNifEnv* env, ERL_NIF_TERM list, BindNameTypeMap* metadata);

#endif /* defined(__erlcass__metadata__) */
