//
//  nif_utils.h
//  erlcass
//
//  Created by silviu on 5/8/15.
//
//

#ifndef __erlcass__utils__
#define __erlcass__utils__

#include "erl_nif.h"
#include "cassandra.h"

#define BIN_TO_STR(x) reinterpret_cast<const char*>(x)

ERL_NIF_TERM make_atom(ErlNifEnv* env, const char* name);
ERL_NIF_TERM make_error(ErlNifEnv* env, const char* error);
ERL_NIF_TERM make_binary(ErlNifEnv* env, const char* buff, size_t length);

ERL_NIF_TERM cass_error_to_nif_term(ErlNifEnv* env, CassError error);
ERL_NIF_TERM cass_future_error_to_nif_term(ErlNifEnv* env, CassFuture* future);

bool parse_consistency_level_options(ErlNifEnv* env, ERL_NIF_TERM options_list, CassConsistency* cl, CassConsistency* serial_cl);
bool parse_query_term(ErlNifEnv* env, ERL_NIF_TERM qterm, ErlNifBinary* query, CassConsistency* cl, CassConsistency* serial_cl);

bool get_bstring(ErlNifEnv* env, ERL_NIF_TERM term, ErlNifBinary* bin);

#endif
