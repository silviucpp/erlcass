//
//  nif_tuple.hpp
//  erlcass
//
//  Created by silviu on 9/23/15.
//
//

#ifndef ERLCASS_C_SRC_NIF_TUPLE_H
#define ERLCASS_C_SRC_NIF_TUPLE_H

#include "erlcass.h"

ERL_NIF_TERM nif_term_to_cass_tuple(ErlNifEnv* env, ERL_NIF_TERM term, const SchemaColumn & type, CassTuple** tuple);

#endif
