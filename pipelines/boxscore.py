import pandas as pd
import polars as pl

from pipelines.base import Pipeline
from connectors.static_data import StaticDataConnector
from transforms.transform_boxscore import Transform


class BoxscorePipeline(Pipeline[dict]):

    def __init__(self, pipeline_name: str, sc_data: dict, environment: str):
        super().__init__(pipeline_name=pipeline_name, pipeline_tag='boxscore', source_tag='NBA static data feed')
        self.GameID = sc_data['GameID']
        self.GameIDStr = sc_data['GameIDStr']
        self.Data = sc_data
        self.url = f'https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{self.GameIDStr}.json'
        self.source = StaticDataConnector(self)
        self.transformer = Transform(self)
        self.environment = environment
        self.file_source = f'tests/box/{self.GameID}'
        
    def extract(self) -> dict:
        '''Summary
        -------------
        Fetches data from NBA's static data feeds
                
        :return data (dict): Dict containing 'meta' and **'game'** dicts

        Example
        ------------
        >>> {"meta": {}, "game":{}}
        '''
        data_extract = self.source.fetch() if self.environment == 'Production' else self.source.fetch_file()
        self.logger.info(f'Extracted {self.GameID} Box data')
        return data_extract


    def transform(self, data_extract: dict) -> dict:
        '''Summary
        -------------
        Transforms extracted Boxscore and Scoreboard/Schedule data into 9 dicts formatted for SQL.

        Also creates start_action_keys and lineup_keys, neccessary for PlayByPlay

        :param dict data_extract: Box data extract
        :return data_transformed: Formatted Box data
        :rtype: dict

        Example
        ------------
        >>> data_transformed = {
            'SeasonID': 2025,
            'GameID': 2025,
            'sql_tables': {
                'Team':[{}],
                'Arena': {},
                'Official': [{}],
                'Player': [{}],
                'Game': {},
                'GameExt': {},
                'TeamBox': [{}],
                'PlayerBox': [{}],
                'StartingLineups': [{}]
                },
            'start_action_keys': {},
            'lineup_keys': {}
        }
        '''
        data_transformed = self.transformer.box(data_extract)
        self.logger.info(f'Transformed Box data to {', '.join(name for name, data in data_transformed['sql_tables'].items())}')
        return data_transformed



    def load(self, data_transformed: dict):
        '''Summary
    -------------
    Calls *initiate_insert()* which executes the SQL upsert process, but just returns transformed data.

    :param dict data_transformed: Transformed Boxscore data ready to be inserted to SQL db.
    :return data_transformed: _description_
    :rtype: _type_

    Example
    ------------
    >>> data_transformed = {
        'SeasonID': 2025,
        'GameID': 2025,
        'sql_tables': {
            'Team':[{}],
            'Arena': {},
            'Official': [{}],
            'Player': [{}],
            'Game': {},
            'GameExt': {},
            'TeamBox': [{}],
            'PlayerBox': [{}],
            'StartingLineups': [{}]
            },
        'start_action_keys': {},
        'lineup_keys': {}
    }
    '''
        data_loaded = self.destination.initiate_insert(data_transformed['sql_tables'])

        return data_transformed