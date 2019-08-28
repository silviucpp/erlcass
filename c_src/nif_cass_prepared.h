#ifndef C_SRC_NIF_CASS_PREPARED_H_
#define C_SRC_NIF_CASS_PREPARED_H_

#include "erl_nif.h"
#include "cassandra.h"
#include "nif_utils.h"

ERL_NIF_TERM nif_cass_prepared_new(ErlNifEnv* env, ErlNifResourceType* rs, const CassPrepared* prep, const ConsistencyLevelOptions& consistency);
ERL_NIF_TERM nif_cass_prepared_bind(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
void nif_cass_prepared_free(ErlNifEnv* env, void* obj);

#endif  // C_SRC_NIF_CASS_PREPARED_H_
