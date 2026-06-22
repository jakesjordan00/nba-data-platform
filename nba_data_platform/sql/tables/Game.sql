
if not exists(
select 1 from sys.tables t where t.name = 'Game'
) begin
create table Game(
SeasonID			int,
GameID				int,
Date				date,
GameType			varchar(10),
HomeID				int,
HScore				int,
AwayID				int,
AScore				int,
WinnerID			int,
WScore				int,
LoserID				int,
LScore				int,
SeriesID			varchar(20),
Datetime			datetime,
Primary Key (SeasonID, GameID),
Foreign Key (SeasonID, HomeID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, AwayID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, WinnerID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, LoserID) references Team(SeasonID, TeamID))
end
