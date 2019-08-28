#ifndef C_SRC_NIF_CASS_CLUSTER_H_
#define C_SRC_NIF_CASS_CLUSTER_H_

#include "erl_nif.h"

ERL_NIF_TERM nif_cass_cluster_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_cluster_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM nif_cass_log_set_level(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_log_set_callback(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_cluster_set_options(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif  // C_SRC_NIF_CASS_CLUSTER_H_
