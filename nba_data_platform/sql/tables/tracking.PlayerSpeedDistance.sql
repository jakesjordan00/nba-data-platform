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
where t.name = 'PlayerSpeedDistance' and s.name = 'tracking'
)
begin
create table tracking.PlayerSpeedDistance(
SeasonID          int,
GameID            int,
TeamID            int,
MatchupID         int,
PlayerID          int,
DistFeet          decimal(18,2),
DistMiles         decimal(18,2),
DistMilesOff      decimal(18,2),
DistMilesDef      decimal(18,2),
AvgSpeed          decimal(18,2),
AvgSpeedOff       decimal(18,2),
AvgSpeedDef       decimal(18,2),
Primary Key(SeasonID, GameID, TeamID, MatchupID, PlayerID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, PlayerID) references Player(SeasonID, PlayerID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID, PlayerID) references PlayerBox(SeasonID, GameID, TeamID, MatchupID, PlayerID))
end