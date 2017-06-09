#ifndef ERLCASS_C_SRC_NIF_TUPLE_H
#define ERLCASS_C_SRC_NIF_TUPLE_H

#include "erlcass.h"

namespace cass {
    class DataType;
}

ERL_NIF_TERM nif_term_to_cass_tuple(ErlNifEnv* env, ERL_NIF_TERM term, const cass::DataType* data_type, CassTuple** tuple);

#endif
