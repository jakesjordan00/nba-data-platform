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
where t.name = 'TeamCatchShoot' and s.name = 'tracking'
)
begin
create table tracking.TeamCatchShoot(
SeasonID          int,
GameID            int,
TeamID            int,
MatchupID         int,
FGM               int,
FGA               int,
[FG%]             decimal(18,3),
PTS               int,
FG3M              int,
FG3A              int,
[FG3%]            decimal(18,3),
[EFG%]            decimal(18,3),
Primary Key(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID))
end