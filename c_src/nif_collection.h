//
//  collection.h
//  erlcass
//
//  Created by silviu on 5/28/15.
//
//

#ifndef __erlcass__collection__
#define __erlcass__collection__

#include "erl_nif.h"
#include "cassandra.h"
#include "schema.h"

CassCollection* nif_list_to_cass_collection(ErlNifEnv* env, ERL_NIF_TERM list, const SchemaColumn & type, CassError* error);

#endif /* defined(__erlcass__collection__) */
