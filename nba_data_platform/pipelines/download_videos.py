import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nba_data_platform.connectors import FileSystem
import nba_data_platform.config.api_map as map
from nba_data_platform.pipelines import Pipeline
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
        self.sql = self.destination


    def extract(self):
        data_extract = self.sql.query_to_dataframe(self.sql.queries.download_videos)
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
                'GamePageLink': row['GamePageLink'],
                'VideoPageLink': row['VideoPageLink'],
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