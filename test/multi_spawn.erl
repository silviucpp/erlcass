-module(multi_spawn).
-author("silviu.caragea").

-export([do_work/2]).

do_work(Fun, Count) ->
    process_flag(trap_exit, true),
    spawn_childrens(Fun, Count),
    wait_responses(Count).

spawn_childrens(_Fun, 0) ->
    ok;
spawn_childrens(Fun, Count) ->
    spawn_link(Fun),
    spawn_childrens(Fun, Count -1).

wait_responses(0) ->
    ok;
wait_responses(Count) ->
    receive
        {'EXIT',_FromPid, _Reason} ->
            wait_responses(Count -1)
    end.