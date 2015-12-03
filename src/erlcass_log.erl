-module(erlcass_log).
-author("silviu.caragea").

-include("erlcass.hrl").

-define(PROC_NAME, erlcass_log_proc).

-export([start_link/0, init/1, send/3, update_function/1]).

start_link() ->
    proc_lib:start_link(?MODULE, init, [self()]).

init(Parent) ->

    LogLevel = case application:get_env(erlcass, log_level) of
       {ok, Level} ->
           Level;
       _ ->
           ?CASS_LOG_WARN
    end,

    ok = erlcass_nif:cass_log_set_level_and_callback(LogLevel, self()),

    true = register(?PROC_NAME, self()),
    proc_lib:init_ack(Parent, {ok, self()}),
    loop(fun(X) -> default_log_function(X) end).

%% API

update_function(Callback) ->
    case erlang:is_function(Callback, 1) of
        true ->
            ?PROC_NAME ! {update_fun, Callback},
            ok;
        false ->
            badarg
    end.

send(Severity, Msg, Args) ->
    ?PROC_NAME ! {log_message_recv, {Severity, Msg, Args}}.

loop(Fun) ->
    receive

        {log_message_recv, Msg} ->
            try
                Fun(Msg)
            catch
                _:_ -> ok
            end,
            loop(Fun);

        {update_fun, NewFun} ->
            loop(NewFun);
        stop ->
            true
    end.

default_log_function({_Severity, Msg, Args}) ->
    io:format(<<Msg/binary, " ~n">>, Args);

default_log_function(Msg) ->
    io:format(<<"Log received ts: ~p severity: ~s (~p) file: ~s line: ~p function: ~s message: ~s ~n">>, [
        Msg#log_msg.ts,
        Msg#log_msg.severity_str,
        Msg#log_msg.severity,
        Msg#log_msg.file,
        Msg#log_msg.line,
        Msg#log_msg.function,
        Msg#log_msg.message]).