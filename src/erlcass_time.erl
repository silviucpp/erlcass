-module(erlcass_time).
-author("silviu.caragea").

-export([
    date_from_epoch/1,
    time_from_epoch/1,
    date_time_to_epoch/2
]).

-spec date_from_epoch(integer()) -> integer().

date_from_epoch(EpochSecs) ->
    erlcass_nif:cass_date_from_epoch(EpochSecs).

-spec time_from_epoch(integer()) -> integer().

time_from_epoch(EpochSecs) ->
    erlcass_nif:cass_time_from_epoch(EpochSecs).

-spec date_time_to_epoch(integer(), integer()) -> integer().

date_time_to_epoch(Date, Time) ->
    erlcass_nif:cass_date_time_to_epoch(Date, Time).