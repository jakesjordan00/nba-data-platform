
if not exists(
select * 
from sys.tables t 
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = 'PlayerBox' and s.name = 'adv'
) 
begin
create table adv.PlayerBox(
SeasonID					int,
GameID						int,
TeamID						int,
MatchupID					int,
PlayerID					int,
Minutes						decimal(18,1),
OffRTG						decimal(18,1),
DefRTG						decimal(18,1),
NetRTG						decimal(18,1),
[Ast%]						decimal(18,3),
ATR							decimal(18,1),
AstRatio					decimal(18,1),
[OReb%]						decimal(18,3),
[DReb%]						decimal(18,3),
[Reb%]						decimal(18,3),
[TeamTO%]					decimal(18,1),
[EFG%]						decimal(18,3),
[TS%]						decimal(18,3),
[Usage%]					decimal(18,3),
Pace						decimal(18,2),
PacePer40					decimal(18,2),
PIE							decimal(18,3),
POSS						int,
FGM							int,
FGA							int,
[FG%]						decimal(18,3),
Primary Key(SeasonID, GameID, TeamID, MatchupID, PlayerID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, PlayerID) references Player(SeasonID, PlayerID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID, PlayerID) references PlayerBox(SeasonID, GameID, TeamID, MatchupID, PlayerID))
end
go