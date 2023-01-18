% logs

-include_lib("kernel/include/logger.hrl").

% timeouts

-define(RESPONSE_TIMEOUT, 20000).
-define(CONNECT_TIMEOUT, 5000).

% data types

-type reason() :: term().
