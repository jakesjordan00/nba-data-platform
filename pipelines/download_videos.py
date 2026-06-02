import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors import FileSystem, APIDataConnector
import config.api_map as map
from pipelines import Pipeline
import polars as pl
import requests

class DownloadVideos(Pipeline):            
    def __init__(self, query: str = ''):
        super().__init__(pipeline_name='download-videos', pipeline_tag='downloader', source_tag='Event Video Downloader')
        self.headers = {
            **map.stats_headers,
        }
        self.base_url = 'https://stats.nba.com/stats/videoeventsasset'
        self.params = {
            "GameID": '',
            "GameEventID": ''
        }
        self.filesys = FileSystem(self)
        self.source = self.destination
        
        self.query = """
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
from PlayByPlay pbp
left join Player p on pbp.SeasonID = p.SeasonID and pbp.PlayerID = p.PlayerID
inner join Game g on pbp.SeasonID = g.SeasonID and pbp.GameID = g.GameID
inner join GameExtTime e on pbp.SeasonID = e.SeasonID and pbp.GameID = e.GameID
where pbp.SeasonID = 2025
and p.Name = 'Rudy Gobert' --and pbp.ActionType = 'freethrow' 
and ShotResult = 'Missed'
order by Date desc

""" if query == '' else query


    def extract(self):
        data_extract = self.source.query_db(self.query)
        return data_extract
    

    def transform(self, data_extract: pl.DataFrame):
        data_transformed = data_extract.to_dicts()
        actions = {
            (row['GameID'], row['ActionNumber']): {
                'PlayerID':  row['PlayerID'],
                'Description': row['Description'],
                'PointInGame': row['PointInGame'],
                'Qtr': row['Qtr'],
                'Clock': row['Clock'],
                'params': {
                    'GameID': f'00{row['GameID']}',
                    'GameEventID': row['ActionNumber']
                },
            }
            for row in data_transformed
        }
        return actions
    
    def load(self, actions: dict):
        self.filesys.download_videos(actions)
        bp = 'here'