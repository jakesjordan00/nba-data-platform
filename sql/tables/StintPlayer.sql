
if not exists(
select 1 from sys.tables t where t.name = 'StintPlayer'
) begin
create table StintPlayer(
SeasonID int not null,
GameID int not null,
TeamID int not null,
StintID int not null,
PlayerID int not null,
MinutesPlayed float,
PlusMinus int,
PTS int,
AST int,
REB int,
FG2M int,
FG2A int,
FG3M int,
FG3A int,
FGM int,
FGA int,
FTM int,
FTA int,
OREB int,
DREB int,
TOV int,
STL int,
BLK int,
BLKd int,
F int,
FDrwn int,
primary key(SeasonID, GameID, TeamID, StintID, PlayerID),
foreign key(SeasonID, GameID, TeamID, StintID) references Stint(SeasonID, GameID, TeamID, StintID))
end