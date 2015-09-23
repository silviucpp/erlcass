//
//  nif_tuple.hpp
//  erlcass
//
//  Created by silviu on 9/23/15.
//
//

#ifndef nif_tuple_hpp
#define nif_tuple_hpp

#include "erl_nif.h"
#include "cassandra.h"
#include "metadata.h"

CassTuple* nif_term_to_cass_tuple(ErlNifEnv* env, ERL_NIF_TERM term, const SchemaColumn & type, CassError* error);

#endif /* nif_tuple_hpp */
