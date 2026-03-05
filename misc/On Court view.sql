
with TeamsOnCourt as(
select s.*
         , dense_rank() over(partition by TeamID order by StintID desc) OnCourt
from Stint s
where s.SeasonID = 2025 and s.GameID in(22500825)
)
select c.SeasonID, c.GameID, c.TeamID, t.Name , c.StintID, c.QtrStart, c.ClockStart, c.MinutesPlayed, c.Possessions, c.PtsScored, c.PtsAllowed
, c.FG2M, c.FG2A, c.FG3M, c.FG3A, c.FTM, c.FTA
, c.OREB, c.DREB, c.TOV, c.stl, c.BLK, c.BLKd, c.F, c.FDrwn
from TeamsOnCourt c 
inner join Team t on c.SeasonID = t.SeasonID and c.TeamID = t.TeamID
where OnCourt = 1
order by OnCourt asc, GameID, c.TeamID
go

with PlayersOnCourt as(
select sp.*
         , dense_rank() over(partition by TeamID order by StintID desc) OnCourt
from StintPlayer sp
where sp.SeasonID = 2025 and sp.GameID in(22500825)
)
select poc.SeasonID, poc.GameID, poc.TeamID, poc.StintID
, poc.PlayerID
, case when len(p.number) = 1 then concat(p.number, '.  ', p.Name) else concat(p.number, '. ', p.Name) end Name
, concat('Q',s.QtrStart, ' ', replace(s.ClockStart, '.00', '')) Start
, poc.MinutesPlayed, poc.PlusMinus, poc.PTS, poc.AST, poc.REB, poc.FG2M, poc.FG2A, poc.FG3M, poc.FG3A, poc.FGM, poc.FGA, poc.FTM, poc.FTA
, poc.OREB, poc.DREB, poc.TOV, poc.stl, poc.BLK, poc.BLKd, poc.F, poc.FDrwn
from PlayersOnCourt poc
inner join Player p on poc.SeasonID = p.SeasonID and poc.PlayerID = p.PlayerID
inner join Stint s on poc.SeasonID = s.SeasonID and poc.GameID = s.GameID and poc.TeamID = s.TeamID and poc.StintID = s.StintID
where OnCourt = 1
order by OnCourt asc, GameID, TeamID, PlayerID
go
