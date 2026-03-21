
with NotDoneInGame as(
/* Using Schedule table, checks Game and GameExt to determine any games not in game or not having a status like Final in GameExt. 
*/
	select s.SeasonID
		 , s.GameID
		 , e.Status
		 , s.GameTimeEST
	from Schedule s
	left join Game g on s.SeasonID = g.SeasonID and s.GameID = g.GameID and s.HomeID = g.HomeID and s.AwayID = g.AwayID
	left join GameExt e on s.SeasonID = e.SeasonID and s.GameID = e.GameID
	where s.SeasonID = 2025 
	and s.GameTimeEST < cast(getdate() as date)
	and (g.GameID is null or e.Status not like '%final%') 
	and left(s.GameID, 1) in(2, 4, 5) --Only get Regular Season, Playoffs and Play-in
),
LastAction as(
/* 
For each game in the PlayByPlay table, find the max ActionID.
	The ActionID returned should correspond to the game's final action up to that point
*/
	select p.SeasonID, p.GameID, max(p.ActionID) LastAction
	from PlayByPlay p
	where SeasonID = 2025 and SubType != 'memo'
	group by p.SeasonID, p.GameID
),
FirstAction as(
/* 
For each game in the PlayByPlay table, find the min ActionID.
	The ActionID should equal 1, in which case it will be filtered. If any games are returned, they are missing data.
*/
	select p.SeasonID, p.GameID, min(p.ActionID) FirstAction
	from PlayByPlay p
	where SeasonID = 2025 and SubType != 'memo'
	group by p.SeasonID, p.GameID
	having min(p.ActionID) != 1
),
NotInPlayByPlay as(
/* 
Games not found in PlayByPlay (Must be found in Game)
*/
	select s.SeasonID, s.GameID sch_GameID, p.GameID pbp_GameID
	from Schedule s
	left join Game g on s.SeasonID = g.SeasonID and s.GameID = g.GameID and s.HomeID = g.HomeID and s.AwayID = g.AwayID
	left join GameExt e on s.SeasonID = e.SeasonID and s.GameID = e.GameID
	left join PlayByPlay p on s.SeasonID = p.SeasonID and s.GameID = p.GameID
	where s.SeasonID = 2025 
	and g.Date < cast(getdate() as date)
	and p.GameID is null
	and left(s.GameID, 1) in(2, 4, 5) --Only get Regular Season, Playoffs and Play-in
)
select la.SeasonID
	 , la.GameID
from LastAction la
inner join PlayByPlay p on la.SeasonID = p.SeasonID and p.GameID = la.GameID and la.LastAction = p.ActionID
where (ActionType != 'game' and SubType != 'end') or (Clock != '00:00.00' and Qtr < 4)
union
select p.SeasonID
	 , p.sch_GameID GameID
from NotInPlayByPlay p
union
select p.SeasonID
	 , p.GameID
from NotDoneInGame p
union
select f.SeasonID
	 , f.GameID
from FirstAction f
