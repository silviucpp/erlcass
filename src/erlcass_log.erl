-module(erlcass_log).
-author("silviu.caragea").

-include("erlcass.hrl").

-export([start_link/0, init/1, send/4, update_function/2]).

start_link() ->
    proc_lib:start_link(?MODULE, init, [self()]).

update_function(LogPid, Callback) ->
    case erlang:is_function(Callback, 1) of
        true ->
            LogPid ! {update_fun, Callback};
        _ ->
            case Callback of
                undefined ->
                    %switch back to default function
                    LogPid ! {update_fun, fun(X) -> default_log_function(X) end};
                _ ->
                    badarg
            end
    end.

send(LogPid, Severity, Msg, Args) ->
    LogPid ! {log_message_recv, {Severity, Msg, Args}}.

%internals

init(Parent) ->

    LogLevel = case application:get_env(erlcass, log_level) of
       {ok, Level} ->
           Level;
       _ ->
           ?CASS_LOG_WARN
    end,

    ok = erlcass_nif:cass_log_set_level_and_callback(LogLevel, self()),
    ok = proc_lib:init_ack(Parent, {ok, self()}),
    loop(fun(X) -> default_log_function(X) end).

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
            loop(NewFun)
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
        Msg#log_msg.message
    ]).