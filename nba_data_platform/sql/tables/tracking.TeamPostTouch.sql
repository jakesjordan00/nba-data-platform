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
where t.name = 'TeamPostTouch' and s.name = 'tracking'
)
begin
create table tracking.TeamPostTouch(
SeasonID          int,
GameID            int,
TeamID            int,
MatchupID         int,
TotalTouches      int,
PostTouches       int,
FGM               int,
FGA               int,
[FG%]             decimal(18,3),
FTM               int,
FTA               int,
[FT%]             decimal(18,3),
Pts               int,
Passes            int,
Ast               int,
TOV               int,
Fouls             int,
[%ofPts]            decimal(18,3),
[Pass%]           decimal(18,3),
[Ast%]            decimal(18,3),
[TOV%]            decimal(18,3),
[Foul%]           decimal(18,3),
Primary Key(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID))
end