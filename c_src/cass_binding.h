#ifndef C_SRC_CASS_BINDING_H_
#define C_SRC_CASS_BINDING_H_

#include "cassandra.h"
#include "erl_nif.h"

namespace datastax { namespace internal { namespace core {
class DataType;
}}}

ERL_NIF_TERM cass_bind_by_index(ErlNifEnv* env, CassStatement* statement, size_t index, const datastax::internal::core::DataType* data_type, ERL_NIF_TERM value);

#endif  // C_SRC_CASS_BINDING_H_
