from pipelines.base import get_logger
from connectors import SQLConnector

class PlayerPositionPipeline:
    
    def __init__(self):
        self.pipeline_name = 'player_positions'
        self.sql = SQLConnector(self.pipeline_name, 'JJsNBA')
        self.query = '''
with Ranks as(
select s.SeasonID
	 , s.PlayerID
	 , p.Name
	 , s.Position
	 , count(s.Position) Starts
	 , ROW_NUMBER() over(partition by s.SeasonID, s.PlayerID order by count(s.Position) desc, s.Position) PositionRank
from StartingLineups s
inner join Player p on s.SeasonID = p.SeasonID and s.PlayerID = p.PlayerID
where s.SeasonID = 2025 
and s.Position is not null
group by s.SeasonID, s.PlayerID, p.Name, s.Position
)
update p
set p.Position = r.Position
from Player p 
inner join Ranks r on p.SeasonID = r.SeasonID and p.PlayerID = r.PlayerID
where PositionRank = 1
'''
        self.logger = get_logger(self.pipeline_name)
        pass



    def run(self):
        self.logger.info('Spinning up...')
        rows_affected = self.sql.raw_execute(self.query)
        return_msg = {
            'rows_affected': rows_affected
        }
        self.logger.info(f'{rows_affected} rows affected')
        return return_msg
    


