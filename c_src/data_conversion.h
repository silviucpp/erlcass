//
//  data_conversion.h
//  erlcass
//
//  Created by silviu on 5/13/15.
//
//

#ifndef __erlcass__data_conversion__
#define __erlcass__data_conversion__

#include "erl_nif.h"
#include "cassandra.h"

ERL_NIF_TERM cass_result_to_erlang_term(ErlNifEnv* env, const CassResult* result);

#endif /* defined(__erlcass__data_conversion__) */
