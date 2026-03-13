select s.*
	 , pb.PlayerID
	 , case when pb.TeamID = s.HomeID then 'Home' else 'Away' end HomeAway
from Schedule s
left join PlayerBox pb on s.SeasonID = pb.SeasonID and s.GameID = pb.GameID
where s.SeasonID = 2025 and s.GameType != 'PRE' and s.GameTimeEST <= getdate()
order by s.GameTimeEst, s.GameID