with DatesToDo as(
select distinct cast(s.GameTimeEST as date) Date
from Schedule s
left join PlayerBox b on s.SeasonID = b.SeasonID and s.GameID = b.GameID
left join {schema}.{table} pb on b.SeasonID = pb.SeasonID and b.GameID = pb.GameID and b.TeamID = pb.TeamID and b.MatchupID = pb.MatchupID and b.PlayerID = pb.PlayerID
where b.SeasonID = 2025 and left(b.GameID, 1) not in(1, 3, 6) and b.[Minutes] != '00:00.00'
and pb.PlayerID is null
)
select s.*
	 , pb.PlayerID
	 , case when pb.TeamID = s.HomeID then 'Home' else 'Away' end HomeAway
	 , pb.Minutes
	 , pb.MinutesCalculated
from Schedule s
left join PlayerBox pb on s.SeasonID = pb.SeasonID and s.GameID = pb.GameID
where s.SeasonID = 2025 and s.GameType not in('PRE', 'CUP', 'AS') and s.GameTimeEST <= getdate()
and cast(s.GameTimeEST as date) in (select Date from DatesToDo)
order by s.GameTimeEst desc, s.GameID
