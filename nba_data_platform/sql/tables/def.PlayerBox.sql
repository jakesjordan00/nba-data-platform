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
where t.name = 'PlayerBox' and s.name = 'def'
) 
begin
create table def.PlayerBox(
SeasonID          int,
GameID            int,
TeamID            int,
MatchupID         int,
PlayerID          int,
[DReb%]          decimal(18,3),
[%TeamDReb]          decimal(18,3),
[%TeamSTL]          decimal(18,3),
[%TeamBLK]          decimal(18,3),
DefWinShare          decimal(18,3),
DefWinShareRaw          decimal(18,3),
Primary Key(SeasonID, GameID, TeamID, MatchupID, PlayerID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, PlayerID) references Player(SeasonID, PlayerID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID, PlayerID) references PlayerBox(SeasonID, GameID, TeamID, MatchupID, PlayerID))
end