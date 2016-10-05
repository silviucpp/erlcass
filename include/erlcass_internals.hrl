-author("silviu").

%ets tables

-define(ETS_PREPARED_STM_CACHE, erlcass_ets_prepared_stm_cache).
-define(ETS_PREPARED_STM_SESSIONS, erlcass_ets_prepared_stm_sessions).

%logs

-define(PRINT_MSG(Format, Args),
    io:format(Format, Args)).

-define(DEBUG_MSG(Format, Args),
    lager:debug(Format, Args)).

-define(INFO_MSG(Format, Args),
    lager:info(Format, Args)).

-define(WARNING_MSG(Format, Args),
    lager:warning(Format, Args)).

-define(ERROR_MSG(Format, Args),
    lager:error(Format, Args)).

-define(CRITICAL_MSG(Format, Args),
    lager:critical(Format, Args)).

%timeouts

-define(RESPONSE_TIMEOUT, 20000).
-define(CONNECT_TIMEOUT, 5000).
