with Overview as(
select pb.SeasonID
	 , pb.GameID
	 , g.Date
	 , pb.TeamID
	 , pb.MatchupID
	 , pb.PlayerID
	 , p.Name
	 , pb.Minutes
	 , a.PlayerID advPlayerID
	 , case when (pb.MinutesCalculated = 0 and a.PlayerID is null)
			  or a.PlayerID is not null
			then 'Good' 
	   else 'Bad' end Status
from Game g
inner join PlayerBox pb on g.SeasonID = pb.SeasonID and g.GameID = pb.GameID
inner join Player p on pb.SeasonID = p.SeasonID and pb.PlayerID = p.PlayerID
left join def.PlayerBox a on pb.SeasonID = a.SeasonID and pb.GameID = a.GameID and pb.TeamID = a.TeamID and pb.MatchupID = a.MatchupID and pb.PlayerID = a.PlayerID
where p.SeasonID = 2025 and left(pb.GameID, 1) not in(1, 3, 6)
)
select *
from overview
where Status = 'Bad'
order by Date