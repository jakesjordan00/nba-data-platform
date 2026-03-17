
if not exists(
select 1 from sys.tables t where t.name = 'GameExt'
) begin
create table GameExt(
SeasonID			int,
GameID				int,
ArenaID             int,
Attendance			int,
Sellout				int,
Label				varchar(100),
LabelDetail			varchar(100),
OfficialID			int,
Official2ID			int,
Official3ID			int,
OfficialAlternateID int,
Status				varchar(50),
Periods				int,
Primary Key(SeasonID, GameID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, ArenaID) references Arena(SeasonID, ArenaID),
Foreign Key (SeasonID, OfficialID) references Official(SeasonID, OfficialID),
Foreign Key (SeasonID, Official2ID) references Official(SeasonID, OfficialID),
Foreign Key (SeasonID, Official3ID) references Official(SeasonID, OfficialID))
end
