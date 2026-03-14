
if not exists(
select 1 from sys.tables t where t.name = 'Schedule'
) begin
create table Schedule(
SeasonID		int,
GameID			int,
GameType		varchar(10),
Status			varchar(255),
Sequence		int,
GameTimeEST		datetime,
GameTimeUTC		datetime,
GameTimeHome	datetime,
GameTimeAway	datetime,
Day				varchar(255),
Week			int,
Label			varchar(255),
LabelDetail		varchar(255),
IsNeutral		int,
HomeID			int,
HomeWins		int,
HomeLosses		int,
HomeScore		int,
HomeSeed		int,
AwayID			int,
AwayWins		int,
AwayLosses		int,
AwayScore		int,
AwaySeed		int,
IfNecessary		int,
SeriesText		varchar(255),
Primary Key (SeasonID, GameID))
end