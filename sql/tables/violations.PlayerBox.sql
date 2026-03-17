if not exists(
select * 
from sys.tables t 
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = 'PlayerBox' and s.name = 'violations'
) 
begin
create table violations.PlayerBox(
SeasonID			int,
GameID				int,
TeamID				int,
MatchupID			int,
PlayerID			int,
Travel				int,
DblDribble          int,
Inbound				int,
Backcourt			int,
Palming				int,
OffFoul				int,
Off3				int,
OffGoaltend         int,
Def3				int,
DefGoaltend         int,
Charge				int,
Lane				int,
JumpBall			int,
KickedBall          int,
DiscDribble         int,
Primary Key(SeasonID, GameID, TeamID, MatchupID, PlayerID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, PlayerID) references Player(SeasonID, PlayerID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID, PlayerID) references PlayerBox(SeasonID, GameID, TeamID, MatchupID, PlayerID))
end
go

