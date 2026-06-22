if not exists(
select 1 from sys.tables t where t.name = 'Team'
) begin
create table Team(
SeasonID			int,
TeamID				int,
City				varchar(255),
Name				varchar(255),
Tricode				varchar(255),
Wins				int,
Losses				int,
FullName			varchar(255),
Conference          varchar(255),
Division            varchar(255),
Primary Key(SeasonID, TeamID))
end