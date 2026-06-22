
select d.SeasonID
     , d.GameID
     , d.TeamID, t.Name Team
     , d.MatchupID, m.Name Matchup
     , d.PlayerID, p.Name Player, d.Position
     , d.LineupStatus, d.RosterStatus
     , d.Timestamp
     , s.GameTimeEST
from DailyLineups d
inner join Team t on d.SeasonID = t.SeasonID and d.TeamID = t.TeamID
inner join Team m on d.SeasonID = m.SeasonID and d.MatchupID = m.TeamID
inner join Player p on d.SeasonID = p.SeasonID and d.PlayerID = p.PlayerID
left join Game g on d.SeasonID = g.SeasonID and d.GameID = g.GameID
left join Schedule s on d.SeasonID = s.SeasonID and d.GameID = s.GameID
order by d.Timestamp desc, d.TeamID, d.MatchupID
go
