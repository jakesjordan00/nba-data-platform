if not exists(
select *
from sys.schemas s
where s.name = 'misc'
)
exec('create schema misc');
if not exists(
select * 
from sys.tables t 
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = 'TeamBox' and s.name = 'misc'
) 
begin
create table misc.TeamBox(
SeasonID					int,
GameID						int,
TeamID						int,
MatchupID					int,
PtsTurnover					int,
PtsSecondChance				int,
PtsFastBreak				int,
PtsInThePaint				int,
OpPtsTurnover				int,
OpPtsSecondChance			int,
OpPtsFastBreak				int,
OpPtsInThePaint				int,
Primary Key(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID))
end
