-module(erlcass_utils).
-author("silviu").

-export([get_env/1, lookup/2, lookup/3]).

get_env(Key) ->
    application:get_env(erlcass, Key).

lookup(Key, List, Default) ->
    case lists:keyfind(Key, 1, List) of
        {Key, Result} ->
            Result;
        false ->
            Default
    end.

lookup(Key, List) ->
    lookup(Key, List, null).