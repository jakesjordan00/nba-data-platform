import pandas as pd
import polars as pl

from pipelines.base import Pipeline
from connectors.static_data import StaticDataConnector
from transforms.transform_boxscore import Transform


class BoxscorePipeline(Pipeline[dict]):
    '''BoxscorePipeline
===
- Given a game's details from the Scoreboard/Schedule pipeline result, fetches Boxscore data from NBA static data feed. <br>
-  Transforms extracted data to dictionaries matching the format of nine tables in SQL db
    - **Team, Arena, Official, Player, Game, GameExt, TeamBox, PlayerBox, and StartingLineups**



## Functions
    ### ```def __init__```(self, pipeline_name: str, sc_data: dict, environment: str):
        - Inherits logger, destination and run_timestamp from superclass.
        - Sets self.Data equal to Scoreboard/Schedule data for that game.
        - Sets GameID and GameIDStr
        - Sets url, tag, source, transformer, environment and file_source

    ### ```extract```(self):
        https://cdn.nba.com/static/json/liveData/boxscore/boxscore_0022500001.json
        - Fetches game's Boxscore data from NBA static data feed.

    ### ```transform```(self, data_extract) -> dict:
        - Given the extracted box data, transforms data to dictionaries matching the format of nine tables in SQL db


    ### ```load```(self, data_transformed) -> dict:
        - Upserts the trasformed data to SQL db.
        ### Team
            - Expected records: 2
        ### Arena
            - Expected records: 1
        ### Official
            - Expected records: 3-4
        ### Player
            - Expected records: 15-20ish (17.59 on avg)
        ### Game
            - Expected records: 1
        ### GameExt
            - Expected records: 1
        ### TeamBox
            - Expected records: 2
        ### PlayerBox
            - Expected records: 15-20ish (should match Player)
        ### StartingLineups
            - Expected records: 15-20ish (should match Player and PlayerBox)
        

'''

    def __init__(self, pipeline_name: str, sc_data: dict, environment: str):
        super().__init__(pipeline_name=pipeline_name, pipeline_tag='boxscore', source_tag='NBA static data feed')
        self.Data = sc_data
        self.GameID = self.Data['GameID']
        self.GameIDStr = self.Data['GameIDStr']
        self.source = StaticDataConnector(self)
        self.url = self.source.boxscore.replace('GameIDStr', self.GameIDStr)
        self.transformer = Transform(self)
        self.environment = environment
        self.file_source = f'.tests/box/{self.GameID}'
        
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