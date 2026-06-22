
if not exists(
select 1 from sys.tables t where t.name = 'StintStatus'
) begin
create table StintStatus(
SeasonID int not null,
GameID int not null,
Status varchar(20),
primary key(SeasonID, GameID))
end