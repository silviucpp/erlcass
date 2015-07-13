//
//  nif_cass_statement.h
//  erlcass
//
//  Created by silviu on 5/12/15.
//
//

#ifndef __erlcass__nif_cass_statement__
#define __erlcass__nif_cass_statement__

#include "erlcass.h"

CassStatement* get_statement(ErlNifEnv* env, ErlNifResourceType* resource_type, ERL_NIF_TERM arg);
ERL_NIF_TERM nif_cass_statement_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_statement_new(ErlNifEnv* env, ErlNifResourceType* resource_type, const CassPrepared* prep, CassConsistency consistency);
ERL_NIF_TERM nif_cass_statement_bind_parameters(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
void nif_cass_statement_free(ErlNifEnv* env, void* obj);

#endif /* defined(__erlcass__nif_cass_statement__) */
