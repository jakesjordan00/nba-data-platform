if not exists(
select * 
from sys.tables t 
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = 'PlayerBox' and s.name = 'usage'
) 
begin
create table usage.PlayerBox(
SeasonID          int,
GameID            int,
TeamID            int,
MatchupID         int,
PlayerID          int,
[Usage%]          decimal(18,3),
[%TeamFGM]          decimal(18,3),
[%TeamFGA]          decimal(18,3),
[%TeamFG3M]          decimal(18,3),
[%TeamFG3A]          decimal(18,3),
[%TeamFTM]          decimal(18,3),
[%TeamFTA]          decimal(18,3),
[%TeamOREB]          decimal(18,3),
[%TeamDREB]          decimal(18,3),
[%TeamREB]          decimal(18,3),
[%TeamAST]          decimal(18,3),
[%TeamTOV]          decimal(18,3),
[%TeamSTL]          decimal(18,3),
[%TeamBLK]          decimal(18,3),
[%TeamBLKd]          decimal(18,3),
[%TeamPF]          decimal(18,3),
[%TeamPFDrwn]          decimal(18,3),
[%TeamPTS]          decimal(18,3),
Primary Key(SeasonID, GameID, TeamID, MatchupID, PlayerID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, PlayerID) references Player(SeasonID, PlayerID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID, PlayerID) references PlayerBox(SeasonID, GameID, TeamID, MatchupID, PlayerID))
end