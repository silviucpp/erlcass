#ifndef ERLCASS_C_SRC_NIF_CASS_CLUSTER_H
#define ERLCASS_C_SRC_NIF_CASS_CLUSTER_H

#include "erl_nif.h"
#include "cassandra.h"

void nif_cass_cluster_free(ErlNifEnv* env, void* obj);
CassCluster* get_cass_cluster(ErlNifEnv* env, ErlNifResourceType* resource_type, const ERL_NIF_TERM arg);

ERL_NIF_TERM nif_cass_cluster_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_cluster_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM nif_cass_log_set_level(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_log_set_callback(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_cluster_set_options(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif
