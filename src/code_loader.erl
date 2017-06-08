-module(code_loader).

-export([start/0]).

start() ->
    load_code(deps).

load_code(Arg) ->
    case init:get_argument(Arg) of
        {ok,[[Dir]]} ->
            case file:list_dir(Dir) of
                {ok, L} ->
                    io:format(<<"Load deps code from ~p ~n">>,[Dir]),
                    lists:foreach(fun(I) -> Path = Dir ++ "/" ++ I ++ "/ebin", code:add_path(Path) end, L),
                    ok;
                _ ->
                    throw(badarg)
            end;
        _ ->
            ok
    end.

