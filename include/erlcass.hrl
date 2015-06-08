%%%-------------------------------------------------------------------
%%% @author silviu.caragea
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. May 2015 10:08 AM
%%%-------------------------------------------------------------------
-author("silviu.caragea").

-define(CASS_NULL, null).

-define(CASS_CONSISTENCY_ANY, 0).
-define(CASS_CONSISTENCY_ONE, 1).
-define(CASS_CONSISTENCY_TWO, 2).
-define(CASS_CONSISTENCY_THREE, 3).
-define(CASS_CONSISTENCY_QUORUM, 4).
-define(CASS_CONSISTENCY_ALL, 5).
-define(CASS_CONSISTENCY_LOCAL_QUORUM, 6).
-define(CASS_CONSISTENCY_EACH_QUORUM, 7).
-define(CASS_CONSISTENCY_SERIAL, 8).
-define(CASS_CONSISTENCY_LOCAL_SERIAL, 9).
-define(CASS_CONSISTENCY_LOCAL_ONE, 10).

-define(CASS_BATCH_TYPE_LOGGED, 0).
-define(CASS_BATCH_TYPE_UNLOGGED, 1).
-define(CASS_BATCH_TYPE_COUNTER, 2).

-define(CASS_TEXT, text).                                           %use for (ascii, text, varchar)
-define(CASS_INT, int).                                             %use for (int )
-define(CASS_BIGINT, bigint).                                       %use for (timestamp, counter, bigint)
-define(CASS_BLOB, blob).                                           %use for (varint, blob)
-define(CASS_BOOLEAN, bool).                                        %use for (bool)
-define(CASS_FLOAT, float).                                         %use for (float)
-define(CASS_DOUBLE, double).                                       %use for (double)
-define(CASS_INET, inet).                                           %use for (inet)
-define(CASS_UUID, uuid).                                           %use for (timeuuid, uuid)
-define(CASS_DECIMAL, decimal).                                     %use for (decimal)
-define(CASS_LIST(ValueType), {list, ValueType}).                   %use for list
-define(CASS_SET(ValueType), {set, ValueType}).                     %use for set
-define(CASS_MAP(KeyType, ValueType), {map, KeyType, ValueType}).   %use for map
