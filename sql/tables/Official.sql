
if not exists(
select 1 from sys.tables t where t.name = 'Official'
) begin
create table Official(
SeasonID			int,
OfficialID			int,
Name				varchar(255),
Number				varchar(3)
Primary Key(SeasonID, OfficialID))
end
go