-module(erlcass_stm_sessions).

-export([
    get_table_name/1,
    get_existing_table_name/1,
    create/1,
    set/4,
    get/2
]).

table_name(Name) ->
    erlcass_utils:concat_atoms(Name, '_erlcass_ets_prepared_stm_sessions').

get_table_name(Name) ->
    binary_to_atom(table_name(Name), utf8).

get_existing_table_name(Name) ->
    binary_to_existing_atom(table_name(Name), utf8).

create(Name) ->
    Name = ets:new(Name, [set, named_table, protected, {read_concurrency, true}]),
    ok.

set(Name, Identifier, Session, StatementRef) ->
    true = ets:insert(Name, {Identifier, {Session, StatementRef}}).

get(Name, Identifier) ->
    case ets:lookup(Name, Identifier) of
        [{Identifier, Value}] ->
            Value;
        [] ->
            undefined
    end.
