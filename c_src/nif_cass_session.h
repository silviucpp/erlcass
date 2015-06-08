//
//  nif_cass_session.h
//  erlcass
//
//  Created by silviu on 5/8/15.
//
//

#ifndef __erlcass__nif_cass_session__
#define __erlcass__nif_cass_session__

#include "erl_nif.h"

void nif_cass_session_free(ErlNifEnv* env, void* obj);
ERL_NIF_TERM nif_cass_session_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_session_connect(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_session_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_session_prepare(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_session_execute(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_session_execute_batch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_session_get_metrics(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif /* defined(__erlcass__nif_cass_session__) */
