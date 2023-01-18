-module(erlcass_log).

-include("erlcass.hrl").
-include("erlcass_internals.hrl").

-record(log_msg, {severity, file, line, function, message}).

-define(MSG_FORMAT, "~p on ~p at file ~s line ~p").

-export([start_link/0, init/1]).

start_link() ->
    proc_lib:start_link(?MODULE, init, [self()]).

%internals

init(Parent) ->
    Self = self(),
    ok = erlcass_cluster:set_log_process_receiver(Self),
    ok = proc_lib:init_ack(Parent, {ok, Self}),
    loop().

loop() ->
    receive
        {log_message_recv, Msg} ->
            log_message(Msg),
            loop()
    end.

log_message(#log_msg{message = Msg, function = Fun, file = File, line = Line, severity = Severity}) ->
    case Severity of
        ?CASS_LOG_CRITICAL ->
            ?LOG_CRITICAL(?MSG_FORMAT, [Msg, Fun, File, Line]);
        ?CASS_LOG_ERROR ->
            ?LOG_ERROR(?MSG_FORMAT, [Msg, Fun, File, Line]);
        ?CASS_LOG_WARN ->
            ?LOG_WARNING(?MSG_FORMAT, [Msg, Fun, File, Line]);
        ?CASS_LOG_INFO ->
            ?LOG_INFO(?MSG_FORMAT, [Msg, Fun, File, Line]);
        _ ->
            ?LOG_DEBUG(?MSG_FORMAT, [Msg, Fun, File, Line])
    end.
