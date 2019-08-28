#ifndef C_SRC_DATA_CONVERSION_H_
#define C_SRC_DATA_CONVERSION_H_

#include "erl_nif.h"

typedef struct CassTableMeta_ CassTableMeta;
typedef struct CassKeyspaceMeta_ CassKeyspaceMeta;
typedef struct CassSchemaMeta_ CassSchemaMeta;
typedef struct CassResult_ CassResult;

ERL_NIF_TERM cass_table_meta_fields_to_erlang_term(ErlNifEnv* env, const CassTableMeta* meta);
ERL_NIF_TERM cass_keyspace_meta_fields_to_erlang_term(ErlNifEnv* env, const CassKeyspaceMeta* keyspace_meta);
ERL_NIF_TERM cass_schema_meta_fields_to_erlang_term(ErlNifEnv* env, const CassSchemaMeta* schema_meta);

ERL_NIF_TERM cass_result_to_erlang_term(ErlNifEnv* env, const CassResult* result);

#endif  // C_SRC_DATA_CONVERSION_H_
