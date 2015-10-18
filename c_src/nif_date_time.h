//
//  nif_date_time.h
//  erlcass
//
//  Created by silviu on 10/18/15.
//
//

#ifndef nif_date_time_h
#define nif_date_time_h

#include "erl_nif.h"

ERL_NIF_TERM nif_cass_date_from_epoch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_time_from_epoch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_date_time_to_epoch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif /* nif_date_time_h */
