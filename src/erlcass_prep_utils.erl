-module(erlcass_prep_utils).
-author("silviu.caragea").

-define(PREPARED_ETS_TABLE, erlcass_prepared_statements_ets).

-export([create/0, set/2, get/1, does_exist/1]).

create() ->
    ?PREPARED_ETS_TABLE = ets:new(?PREPARED_ETS_TABLE, [set, named_table, protected, {read_concurrency, true}]),
    ok.

set(Identifier, StatementRef) ->
    true = ets:insert(?PREPARED_ETS_TABLE, {Identifier, StatementRef}).

get(Identifier) ->
    case ets:lookup(?PREPARED_ETS_TABLE, Identifier) of
        [{Identifier, Value}] ->
            Value;
        [] ->
            undefined
    end.

does_exist(Identifier) ->
    case ets:lookup(?PREPARED_ETS_TABLE, Identifier) of
        [{Identifier, _}] ->
            true;
        [] ->
            false
    end.
