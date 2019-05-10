-module(erlcass_utils).

-export([
    get_env/1,
    get_env/2,
    lookup/2,
    lookup/3,
    except/2,
    concat_atoms/2
]).

get_env(Key) ->
    application:get_env(erlcass, Key).

get_env(Key, Default) ->
    application:get_env(erlcass, Key, Default).

lookup(Key, List, Default) ->
    case lists:keyfind(Key, 1, List) of
        {Key, Result} ->
            Result;
        false ->
            Default
    end.

lookup(Key, List) ->
    lookup(Key, List, null).

except(Key, List) ->
    lists:filter(fun(Elem) ->
        case Elem of
          {Key, _} -> false;
          _Other -> true
        end
    end, List).

concat_atoms(Atom1, Atom2) ->
    A = atom_to_binary(Atom1, utf8),
    B = atom_to_binary(Atom2, utf8),
    <<A/binary, B/binary>>.