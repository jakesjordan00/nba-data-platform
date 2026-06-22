
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
where t.name = 'TeamPassing' and s.name = 'tracking'
)
begin
create table tracking.TeamPassing(
SeasonID          int,
GameID            int,
TeamID            int,
MatchupID         int,
PassMade          int,
PassRcvd          int,
Ast               int,
AstFT             int,
AstSecondary      int,
AstPotential      int,
AstPointsCreated  int,
AstAdj            int,
[AstToPass%]      decimal(18,3),
[AstToPass%Adj]   decimal(18,3),
Primary Key(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID),)
end