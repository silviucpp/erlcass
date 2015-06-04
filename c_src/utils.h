//
//  utils.h
//  erlcass
//
//  Created by silviu on 5/8/15.
//
//

#ifndef __erlcass__utils__
#define __erlcass__utils__

#include "erl_nif.h"
#include "cassandra.h"
#include <string>

ERL_NIF_TERM make_atom(ErlNifEnv* env, const char* name);
ERL_NIF_TERM make_error(ErlNifEnv* env, const char* error);
ERL_NIF_TERM make_binary(ErlNifEnv* env, const char* buff, size_t length);

ERL_NIF_TERM cass_error_to_nif_term(ErlNifEnv* env, CassError error);
ERL_NIF_TERM cass_future_error_to_nif_term(ErlNifEnv* env, CassFuture* future);

bool get_string(ErlNifEnv* env, ERL_NIF_TERM term, std::string & value);
bool get_atom(ErlNifEnv* env, ERL_NIF_TERM term, std::string & value);

#endif
