


with LiveSchedule as(
select s.SeasonID
	 , s.GameType
	 , min(cast(s.GameTimeEST as date)) SchFirstGame
	 , max(cast(s.GameTimeEST as date)) SchLastGame
from Schedule s
group by s.SeasonID, s.GameType
),
FullSchedule as(
select g.SeasonID
	 , g.GameType
	 , min(g.Date) FirstGame
	 , max(g.Date) LastGame
from Game g
group by g.SeasonID, g.GameType
)
select f.SeasonID
	 , f.GameType
     , case when f.GameType = 'PRE' then 'Pre Season'
            when f.GameType = 'RS' then  'Regular Season'
            when f.GameType = 'PS' then  'Playoffs'
            when f.GameType = 'PI' then  'PlayIn'
            when f.GameType = 'CUP' then  'IST'
            when f.GameType = 'AS' then  'All Star'
            else null 
       end SeasonType
	 , case when l.SchFirstGame is not null and l.SchFirstGame != f.FirstGame
				then l.SchFirstGame
			else f.FirstGame end FirstGame
	 , case when l.SchLastGame is not null and l.SchLastGame != f.LastGame
				then l.SchLastGame
			else f.LastGame end LastGame
from FullSchedule f
left join LiveSchedule l on f.SeasonID = l.SeasonID and f.GameType = l.GameType
order by SeasonID desc, FirstGame, LastGame

