
if not exists(
select 1 from sys.tables t where t.name = 'Player'
) begin
create table Player(
SeasonID			int,
PlayerID			int,
Name				varchar(255),
Number				varchar(3),
Position			varchar(100),
NameInitial			varchar(100),
NameLast			varchar(100),
NameFirst			varchar(100),
Primary Key(SeasonID, PlayerID))
end
