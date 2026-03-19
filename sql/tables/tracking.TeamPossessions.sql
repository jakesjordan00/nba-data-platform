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
where t.name = 'TeamPossessions' and s.name = 'tracking'
)
begin
create table tracking.TeamPossessions(
SeasonID          int,
GameID            int,
TeamID            int,
MatchupID         int,
Pts               int,
Touches           int,
TouchFrontCourt   int,
PossTime          int,
SecPerTouch       int,
DribblePerTouch   int,
PtsPerTouch       int,
TouchElbow        int,
TouchPost         int,
TouchPaint        int,
PtsPerElbowTouch  int,
PtsPerPostTouch   int,
PtsPerPaintTouch  int,
Primary Key(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID))
end