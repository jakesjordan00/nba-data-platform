
if not exists(
select 1 from sys.tables t where t.name = 'DailyLineups'
) 
begin
create table DailyLineups(
SeasonID        int,
GameID          int,
TeamID          int,
MatchupID       int,
PlayerID        int,
Position        varchar(10),
LineupStatus    varchar(20),
RosterStatus    varchar(20),
Timestamp       datetime,
Primary key (SeasonID, GameID, TeamID, MatchupID, PlayerID),
Foreign key(SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign key(SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign key(SeasonID, PlayerID) references Player(SeasonID, PlayerID))
end
