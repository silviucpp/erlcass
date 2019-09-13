-module(benchmark).

-include("erlcass.hrl").
%-include_lib("cqerl/include/cqerl.hrl").
-include_lib("marina/include/marina.hrl").
-include_lib("shackle/include/shackle.hrl").

-define(QUERY, <<"SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14 FROM test_table WHERE col1 = ?">>).
-define(ARG_VAL, <<"hello">>).

-define(DRIVER_ERLCASS, erlcass).
%-define(DRIVER_CQERL, cqerl).
-define(DRIVER_MARINA, marina).

-export([
    run/3,
    start/1
]).

start(Module) ->
    case application:ensure_all_started(Module) of
        {ok, [_H|_T]} ->
            case Module of
                ?DRIVER_ERLCASS ->
                    ok = erlcass:add_prepare_statement(testing_query, {?QUERY, ?CASS_CONSISTENCY_ONE});
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

get_client(?DRIVER_ERLCASS) ->
    undefined;
%%get_client(?DRIVER_CQERL) ->
%%    {ok, Client} = cqerl:get_client(),
%%    Client;
get_client(?DRIVER_MARINA) ->
    undefined;
get_client(_) ->
    throw(invalid_module).

close_client(?DRIVER_ERLCASS, _Client) ->
    ok;
%%close_client(?DRIVER_CQERL, Client) ->
%%    cqerl:close_client(Client);
close_client(?DRIVER_MARINA, _) ->
    ok;
close_client(_, _) ->
    throw(invalid_module).

execute_async(?DRIVER_ERLCASS, _Client) ->
    {ok, _Tag} = erlcass:async_execute(testing_query, [?ARG_VAL]),
    ok;
%%execute_async(?DRIVER_CQERL, Client) ->
%%    true = is_reference(cqerl:send_query(Client, #cql_query{
%%        statement=?QUERY,
%%        values=[{col1, ?ARG_VAL}],
%%        consistency = one
%%    })),
%%    ok;
execute_async(?DRIVER_MARINA, _Client) ->
    {ok, _} = marina:async_reusable_query(?QUERY, #{
        values => [?ARG_VAL],
        consistency_level => ?CONSISTENCY_ONE,
        pid => self(),
        timeout => 5000
    }),
    ok;
execute_async(_Module, _Client) ->
    throw(invalid_module).

receive_response(?DRIVER_ERLCASS) ->
    receive
        {execute_statement_result, _, Rs} ->
            case Rs of
                {ok, _, _} ->
                    ok;
                UnexpectedResult ->
                    UnexpectedResult
            end
    end;
%%receive_response(?DRIVER_CQERL) ->
%%    receive
%%        {result, _Tag, Rs} ->
%%            case is_record(Rs, cql_result) of
%%                true ->
%%                    ok;
%%                _ ->
%%                    Rs
%%            end;
%%        {error, _Tag, Reason} ->
%%            Reason
%%    end;
receive_response(?DRIVER_MARINA) ->
    receive
        {#cast{}, Response} ->
            case marina:response(Response) of
                {ok, _} ->
                    ok;
                UnexpectedResponse ->
                    UnexpectedResponse
            end;
        Msg->
            io:format("received: ~p~n", [Msg]),
            ok
    end;
receive_response(_Module) ->
    throw(invalid_module).

%benchmark internals

consumer_loop(_Module, TotalResults, SuccessResults, FailedResults, 0) ->
%%    io:format("**** Test Completed => module: ~p results: ~p success: ~p failed: ~p **** ~n",[Module,
%%        TotalResults,
%%        SuccessResults,
%%        FailedResults
%%    ]),
    {TotalResults, SuccessResults, FailedResults};

consumer_loop(Module, TotalResults, SuccessResults, FailedResults, LimitReq) ->
    case receive_response(Module) of
        ok ->
            consumer_loop(Module, TotalResults +1 , SuccessResults +1, FailedResults, LimitReq -1);
        UnexpectedResponse ->
            io:format("## Unexpected result: ~p ~n", [UnexpectedResponse]),
            consumer_loop(Module, TotalResults +1 , SuccessResults, FailedResults + 1, LimitReq -1)
    end.

producer_loop(_Module, _Client, 0) ->
    ok;
producer_loop(Module, Client, NumRequests) ->
    ok = execute_async(Module, Client),
    producer_loop(Module, Client, NumRequests -1).

run(Module, NrProcesses, NrReq) ->
    start(Module),

    io:format("Test will start in 1 sec ...~n"),
    timer:sleep(1000),
    io:format("Test started ...~n"),

    ReqPerProcess = round(NrReq/NrProcesses),
    Self = self(),

    Fun = fun() ->
        Client = get_client(Module),
        producer_loop(Module, Client, ReqPerProcess),
        consumer_loop(Module, 0, 0, 0, ReqPerProcess),
        close_client(Module, Client),
        Self ! {self(), done}
    end,

    List = lists:seq(1, NrProcesses),
    {Time, _} = timer:tc(fun()-> bench(Fun, List) end),
    TimeMs = Time/1000,
    io:format("Time to complete: ~p ms req/sec: ~p ~n", [TimeMs, round(NrReq/(TimeMs/1000))]).

bench(Fun, ProcsList) ->
    Pids = [spawn_link(Fun) || _ <- ProcsList],
    [receive {Pid, done} -> ok end || Pid <- Pids],
    ok.