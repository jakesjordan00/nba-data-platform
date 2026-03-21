
if not exists(
select *
from sys.schemas s
where s.name = 'plays'
)
exec('create schema plays');
if not exists(
select *
from sys.tables t
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = 'TeamPlays' and s.name = 'plays'
)
begin
create table plays.TeamPlays(
SeasonID			int,
TeamID				int,
Play                varchar(25),
Type				varchar(10),
GP					int,
Possessions			int,
Frequency			decimal(18,3),
PtsPerPoss			decimal(18,3),
FGM					int,
FGA					int,
PTS					int,
[FG%]				decimal(18,3),
[EFG%]				decimal(18,3),
FTFreq				decimal(18,3),
TOVFreq				decimal(18,3),
FDrwnFreq			decimal(18,3),
And1Freq			decimal(18,3),
ScoreFreq			decimal(18,3),
FGMX				int,
Percentile			decimal(18,3),
FirstDate			datetime,
LastDate			datetime,
Primary Key(SeasonID, TeamID, Play, Type, GP),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID))
end