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
where t.name = 'PlayerRebounding' and s.name = 'tracking'
)
begin
create table tracking.PlayerRebounding(
SeasonID          int,
GameID            int,
TeamID            int,
MatchupID         int,
PlayerID          int,
OReb              int,
ORebContested     int,
ORebUnContested   int,
ORebChances       int,
ORebChanceDefer   int,
AvgORebDist       int,
DReb              int,
DRebContested     int,
DRebUnContested   int,
DRebChances       int,
DRebChanceDefer   int,
AvgDRebDist       int,
Reb               int,
RebContested      int,
RebUnContested    int,
RebChances        int,
RebChanceDefer    int,
AvgRebDist        int,
[ORebContested%]  decimal(18,3),
[ORebChance%]     decimal(18,3),
[ORebChanceAdj%]  decimal(18,3),
[DRebContested%]  decimal(18,3),
[DRebChance%]     decimal(18,3),
[DRebChanceAdj%]  decimal(18,3),
[RebContested%]   decimal(18,3),
[RebChance%]      decimal(18,3),
[RebChanceAdj%]   decimal(18,3),
Primary Key(SeasonID, GameID, TeamID, MatchupID, PlayerID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, PlayerID) references Player(SeasonID, PlayerID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID, PlayerID) references PlayerBox(SeasonID, GameID, TeamID, MatchupID, PlayerID))
end