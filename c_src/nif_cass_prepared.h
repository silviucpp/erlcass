//
//  nif_cass_prepared.h
//  erlcass
//
//  Created by silviu on 5/11/15.
//
//

#ifndef ERLCASS_C_SRC_NIF_CASS_PREPARED_H
#define ERLCASS_C_SRC_NIF_CASS_PREPARED_H

#include "erl_nif.h"
#include "cassandra.h"
#include "nif_utils.h"

ERL_NIF_TERM nif_cass_prepared_new(ErlNifEnv* env, ErlNifResourceType* rs, const CassPrepared* prep, const ConsistencyLevelOptions& consistency);
ERL_NIF_TERM nif_cass_prepared_bind(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
void nif_cass_prepared_free(ErlNifEnv* env, void* obj);

#endif
