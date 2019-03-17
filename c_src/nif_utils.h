#ifndef C_SRC_NIF_UTILS_H_
#define C_SRC_NIF_UTILS_H_

#include "erl_nif.h"
#include "cassandra.h"

struct ConsistencyLevelOptions
{
    ConsistencyLevelOptions() : cl(CASS_CONSISTENCY_ANY), serial_cl(CASS_CONSISTENCY_ANY) {}
    ConsistencyLevelOptions(CassConsistency c, CassConsistency sc) : cl(c), serial_cl(sc) {}

    CassConsistency cl;
    CassConsistency serial_cl;
};

struct QueryTerm
{
    explicit QueryTerm(const ConsistencyLevelOptions& opt) : consistency(opt) {}

    ErlNifBinary query;
    ConsistencyLevelOptions consistency;
};

ERL_NIF_TERM make_atom(ErlNifEnv* env, const char* name);
ERL_NIF_TERM make_error(ErlNifEnv* env, const char* error);
ERL_NIF_TERM make_binary(ErlNifEnv* env, const char* buff, size_t length);
ERL_NIF_TERM make_badarg(ErlNifEnv* env);
ERL_NIF_TERM make_bad_options(ErlNifEnv* env, ERL_NIF_TERM term);

ERL_NIF_TERM cass_error_to_nif_term(ErlNifEnv* env, CassError error);
ERL_NIF_TERM cass_future_error_to_nif_term(ErlNifEnv* env, CassFuture* future);

ERL_NIF_TERM parse_consistency_level_options(ErlNifEnv* env, ERL_NIF_TERM options_list, ConsistencyLevelOptions* cls);
ERL_NIF_TERM parse_query_term(ErlNifEnv* env, ERL_NIF_TERM qterm, QueryTerm* q);

bool get_bstring(ErlNifEnv* env, ERL_NIF_TERM term, ErlNifBinary* bin);
int  get_atom(ErlNifEnv* env, ERL_NIF_TERM term, char* buf, size_t buffer_size);
bool get_boolean(ERL_NIF_TERM term, cass_bool_t* val);

#endif  // C_SRC_NIF_UTILS_H_
