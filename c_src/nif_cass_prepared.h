//
//  nif_cass_prepared.h
//  erlcass
//
//  Created by silviu on 5/11/15.
//
//

#ifndef __erlcass__nif_cass_prepared__
#define __erlcass__nif_cass_prepared__

#include "erlcass.h"

ERL_NIF_TERM nif_cass_prepared_new(ErlNifEnv* env, ErlNifResourceType* resource_type, const CassPrepared* prepared, CassConsistency consistency, BindNameTypeMap* metadata);
ERL_NIF_TERM nif_cass_prepared_bind(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
void nif_cass_prepared_free(ErlNifEnv* env, void* obj);

#endif /* defined(__erlcass__nif_cass_prepared__) */
