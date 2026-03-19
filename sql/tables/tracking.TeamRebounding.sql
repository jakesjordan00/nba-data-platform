if not exists(
select *
from sys.schemas s
where s.name = 'tracking'
)
exec('create schema tracking');
if not exists(
select *
from sys.tables t
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = 'TeamRebounding' and s.name = 'tracking'
)
begin
create table tracking.TeamRebounding(
SeasonID          int,
GameID            int,
TeamID            int,
MatchupID         int,
OReb              int,
ORebContested     int,
ORebUnContested   int,
[ORebContested%]  decimal(18,3),
ORebChances       int,
[ORebChance%]     decimal(18,3),
ORebChanceDefer   int,
[ORebChanceAdj%]  decimal(18,3),
AvgORebDist       int,
DReb              int,
DRebContested     int,
DRebUnContested   int,
[DRebContested%]  decimal(18,3),
DRebChances       int,
[DRebChance%]     decimal(18,3),
DRebChanceDefer   int,
[DRebChanceAdj%]  decimal(18,3),
AvgDRebDist       int,
Reb               int,
RebContested      int,
RebUnContested    int,
[RebContested%]   decimal(18,3),
RebChances        int,
[RebChance%]      decimal(18,3),
RebChanceDefer    int,
[RebChanceAdj%]   decimal(18,3),
AvgRebDist        int,
Primary Key(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID))
end