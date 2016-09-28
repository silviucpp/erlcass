//
//  collection.h
//  erlcass
//
//  Created by silviu on 5/28/15.
//
//

#ifndef ERLCASS_C_SRC_NIF_COLLECTION_H
#define ERLCASS_C_SRC_NIF_COLLECTION_H

#include "erl_nif.h"
#include "metadata.h"

ERL_NIF_TERM nif_list_to_cass_collection(ErlNifEnv* env, ERL_NIF_TERM list, const  SchemaColumn& type, CassCollection ** col);

#endif
