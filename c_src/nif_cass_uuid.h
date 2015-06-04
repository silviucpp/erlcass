//
//  nif_cass_uuid.h
//  erlcass
//
//  Created by silviu on 6/2/15.
//
//

#ifndef __erlcass__nif_cass_uuid__
#define __erlcass__nif_cass_uuid__

#include "erl_nif.h"

ERL_NIF_TERM nif_cass_uuid_gen_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_uuid_gen_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_uuid_gen_random(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_uuid_gen_from_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

ERL_NIF_TERM nif_cass_uuid_min_from_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_uuid_max_from_time(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_uuid_timestamp(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_uuid_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

void nif_cass_uuid_gen_free(ErlNifEnv* env, void* obj);



#endif /* defined(__erlcass__nif_cass_uuid__) */
