//
//  collection.h
//  erlcass
//
//  Created by silviu on 5/28/15.
//
//

#ifndef __erlcass__collection__
#define __erlcass__collection__

#include "erl_nif.h"
#include "metadata.h"

ERL_NIF_TERM nif_list_to_cass_collection(ErlNifEnv* env, ERL_NIF_TERM list, const  SchemaColumn& type, CassCollection ** col);

#endif /* defined(__erlcass__collection__) */
