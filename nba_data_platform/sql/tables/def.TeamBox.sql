
if not exists(
select *
from sys.schemas s
where s.name = 'def'
)
exec('create schema def');
if not exists(
select *
from sys.tables t
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = 'TeamBox' and s.name = 'def'
)
begin
create table def.TeamBox(
SeasonID          int,
GameID            int,
TeamID            int,
MatchupID         int,
[DReb%]           decimal(18,3),
Primary Key(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID))
end