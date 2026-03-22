
if not exists(
select *
from sys.schemas s
where s.name = 'ffactors'
)
exec('create schema ffactors');
if not exists(
select *
from sys.tables t
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = 'TeamBox' and s.name = 'ffactors'
)
begin
create table ffactors.TeamBox(
SeasonID          int,
GameID            int,
TeamID            int,
MatchupID         int,
[EFG%]            decimal(18,3),
FTARate           decimal(18,3),
[TOV%]            decimal(18,3),
[OReb%]           decimal(18,3),
[OpEFG%]          decimal(18,3),
OpFTARate         decimal(18,3),
[OpTOV%]          decimal(18,3),
[OpOReb%]         decimal(18,3),
Primary Key(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID))
end