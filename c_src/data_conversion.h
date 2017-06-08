#ifndef ERLCASS_C_SRC_DATA_CONVERSION_H
#define ERLCASS_C_SRC_DATA_CONVERSION_H

#include "erl_nif.h"

typedef struct CassResult_ CassResult;

ERL_NIF_TERM cass_result_to_erlang_term(ErlNifEnv* env, const CassResult* result);

#endif
