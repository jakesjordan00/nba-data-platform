
with GameExtTime as(
select *
	 , case when e.Periods = 4 then 48 
	   else	48 + (5 * (e.Periods - 4)) end GameTime
	 , Periods - 4 OTs
from GameExt e
)
select p.Name
	 , pbp.*
	 , cast((case when Qtr <= 4 
				then 12 - CAST(LEFT(Clock, 2) + cast(Right(Clock, 5)as decimal(18, 2))/60 as decimal(18,2)) + ((Qtr - 1) * 12)
			when Qtr >= 5
				then (5 - CAST(LEFT(Clock, 2) + cast(Right(Clock, 5)as decimal(18, 2))/60 as decimal(18,2))) + ((((Qtr - 1) - 4) * 5) + 48)
	   else null end / GameTime * 100) as decimal(18, 2)) PointInGame
     , g.Date
     , concat('https://www.nba.com/game/sas-vs-min-', pbp.GameID, '/play-by-play') GamePageLink
	 , case when pbp.SeasonID >= 2014 then 
	 concat('https://www.nba.com/stats/events?CFID=&CFPARAMS=&GameEventID=', pbp.ActionNumber, '&GameID=00', pbp.GameID, 
'&Season=', pbp.SeasonID, '-', pbp.SeasonID - 2000 + 1, 
'&flag=1', '&title=',replace(REPLACE(replace(description, concat(Left(p.Name, 1), '. '), ''), ' ', '%20'), 'S.%20', ''))
	   else null end VideoPageLink
from PlayByPlay pbp
inner join Game g on pbp.SeasonID = g.SeasonID and pbp.GameID = g.GameID
inner join GameExtTime e on pbp.SeasonID = e.SeasonID and pbp.GameID = e.GameID
left join Player p on pbp.SeasonID = p.SeasonID and pbp.PlayerID = p.PlayerID
where pbp.SeasonID = 2025
and p.Name = 'Rudy Gobert' --and pbp.ActionType = 'freethrow' 
and ShotResult = 'Missed'
order by Date desc