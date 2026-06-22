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
where t.name = 'PlayerHustle' and s.name = 'tracking'
)
begin
create table tracking.PlayerHustle(
SeasonID          int,
GameID            int,
TeamID            int,
MatchupID         int,
PlayerID          int,
ContestedShots    int,
ContestedFG2      int,
ContestedFG3      int,
Deflections       int,
ChargesDrawn      int,
ScreenAst         int,
ScreenAstPts      int,
OffLooseBallsRec  int,
DefLooseBallsRec  int,
LooseBallsRec     int,
OffBoxouts        int,
DefBoxouts        int,
Boxouts           int,
BoxoutTeamReb     int,
BoxoutPersReb     int,
[LooseBallsRecOff%]decimal(18,3),
[LooseBallsRecDef%]decimal(18,3),
[OffBoxout%]      decimal(18,3),
[DefBoxout%]      decimal(18,3),
[BoxoutTeamReb%]  decimal(18,3),
[BoxoutPersReb%]  decimal(18,3),
Primary Key(SeasonID, GameID, TeamID, MatchupID, PlayerID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, PlayerID) references Player(SeasonID, PlayerID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID, PlayerID) references PlayerBox(SeasonID, GameID, TeamID, MatchupID, PlayerID))
end