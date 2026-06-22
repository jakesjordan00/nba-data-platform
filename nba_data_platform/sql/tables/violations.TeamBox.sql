
if not exists(
select *
from sys.schemas s
where s.name = 'violations'
)
exec('create schema violations');
if not exists(
select *
from sys.tables t
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = 'TeamBox' and s.name = 'violations'
)
begin
create table violations.TeamBox(
SeasonID          int,
GameID            int,
TeamID            int,
MatchupID         int,
Travel            int,
DblDribble        int,
Inbound           int,
Backcourt         int,
Palming           int,
OffFoul           int,
Off3              int,
OffGoaltend       int,
Def3              int,
DefGoaltend       int,
FiveSec           int,
EightSec          int,
ShotClock         int,
Charge            int,
DelayOfGame       int,
Lane              int,
JumpBall          int,
KickedBall        int,
DiscDribble       int,
Primary Key(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID))
end