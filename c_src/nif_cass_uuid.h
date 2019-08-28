#ifndef C_SRC_NIF_CASS_UUID_H_
#define C_SRC_NIF_CASS_UUID_H_

#include "erl_nif.h"

ERL_NIF_TERM nif_cass_uuid_gen_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_uuid_gen_random(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_uuid_gen_from_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_uuid_min_from_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_uuid_max_from_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_uuid_timestamp(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_uuid_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif  // C_SRC_NIF_CASS_UUID_H_
