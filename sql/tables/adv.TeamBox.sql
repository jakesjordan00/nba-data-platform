if not exists(
select *
from sys.schemas s
where s.name = 'adv'
)
exec('create schema adv');
if not exists(
select * 
from sys.tables t 
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = 'TeamBox' and s.name = 'adv'
) 
begin
create table adv.TeamBox(
SeasonID          int,
GameID            int,
TeamID            int,
MatchupID         int,
OffRTG            decimal(18,1),
DefRTG            decimal(18,1),
NetRTG            decimal(18,1),
[Ast%]            decimal(18,3),
ATR               int,
AstRatio          int,
[OReb%]           decimal(18,3),
[DReb%]           decimal(18,3),
[Reb%]            decimal(18,3),
[TeamTO%]         decimal(18,3),
EOffRTG           decimal(18,1),
EDefRTG           decimal(18,1),
ENetRTG           decimal(18,1),
[EFG%]            decimal(18,3),
[TS%]             decimal(18,3),
Pace              int,
PacePer40         int,
PIE               int,
Possessions       int,
Primary Key(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID))
end