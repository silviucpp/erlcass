//
//  nif_cass_cluster.h
//  erlcass
//
//  Created by silviu on 5/8/15.
//
//

#ifndef ERLCASS_C_SRC_NIF_CASS_CLUSTER_H
#define ERLCASS_C_SRC_NIF_CASS_CLUSTER_H

#include "erl_nif.h"

ERL_NIF_TERM nif_cass_cluster_create(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_cluster_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM nif_cass_log_set_level_and_callback(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_cluster_set_options(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif
