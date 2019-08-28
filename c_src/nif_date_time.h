#ifndef C_SRC_NIF_DATE_TIME_H_
#define C_SRC_NIF_DATE_TIME_H_

#include "erl_nif.h"

ERL_NIF_TERM nif_cass_date_from_epoch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_time_from_epoch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_cass_date_time_to_epoch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

#endif  // C_SRC_NIF_DATE_TIME_H_
