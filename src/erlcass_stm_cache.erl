-module(erlcass_stm_cache).

-export([
    get_table_name/1,
    get_existing_table_name/1,
    create/1,
    set/3,
    find/2,
    to_list/1
]).

table_name(Name) ->
    erlcass_utils:concat_atoms(Name, '_erlcass_ets_prepared_stm_cache').

get_table_name(Name) ->
    binary_to_atom(table_name(Name), utf8).

get_existing_table_name(Name) ->
    binary_to_existing_atom(table_name(Name), utf8).

create(Name) ->
    Name = ets:new(Name, [set, named_table, public, {read_concurrency, true}]),
    ok.

set(Name, Identifier, Query) ->
    true = ets:insert(Name, {Identifier, Query}).

find(Name, Identifier) ->
    case ets:lookup(Name, Identifier) of
        [{Identifier, _Query}] ->
            true;
        [] ->
            false
    end.

to_list(Name) ->
    ets:tab2list(Name).