
if not exists(
select 1 from sys.tables t where t.name = 'Stint'
) begin
create table Stint(
SeasonID int not null,
GameID int not null,
TeamID int not null,
StintID int not null,
QtrStart int,
QtrEnd int,
ClockStart varchar(10),
ClockEnd varchar(10),
MinElapsedStart float,
MinElapsedEnd float,
MinutesPlayed float,
Possessions int,
PtsScored int,
PtsAllowed int,
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
REB int,
AST int,
TOV int,
STL int,
BLK int,
BLKd int,
F int,
FDrwn int,
primary key(SeasonID, GameID, TeamID, StintID))
end