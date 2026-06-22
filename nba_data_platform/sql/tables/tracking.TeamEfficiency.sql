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
where t.name = 'TeamEfficiency' and s.name = 'tracking'
)
begin
create table tracking.TeamEfficiency(
SeasonID          int,
GameID            int,
TeamID            int,
MatchupID         int,
DrivePts          int,
CatchShootPts     int,
PullUpPts         int,
PaintTouchPts     int,
PostTouchPts      int,
ElbowTouchPts     int,
[DriveFG%]        decimal(18,3),
[CatchShootFG%]   decimal(18,3),
[PullUpFG%]       decimal(18,3),
[PaintTouchFG%]   decimal(18,3),
[PostTouchFG%]    decimal(18,3),
[ElbowTouchFG%]   decimal(18,3),
[EFG%]            decimal(18,3),
Primary Key(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID))
end