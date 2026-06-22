import pandas as pd
import polars as pl

from nba_data_platform.pipelines.base import Pipeline
from nba_data_platform.connectors.static_data import StaticDataConnector
from nba_data_platform.transforms.transform_boxscore import Transform


class BoxscorePipeline(Pipeline[dict]):
    '''`BoxscorePipeline`(Pipeline)
    ---
    <hr>
    
    - Given a game's details from the Scoreboard/Schedule pipeline result, fetches Boxscore data from NBA static data feed. <br>
    - Transforms extracted data to dictionaries matching the format of nine tables in SQL db
        - **Team, Arena, Official, Player, Game, GameExt, TeamBox, PlayerBox, and StartingLineups** here
    
    # Extraction
    :meth:`~extract` -> :class:`~connectors.static_data.StaticDataConnector`.:meth:`~connectors.static_data.StaticDataConnector.fetch`
    - Fetches a single game's Boxscore data from NBA's static data feeds

    # Transformation
    :meth:`~transform` -> :class:`~transforms.transform_boxscore.Transform`.:meth:`~transforms.transform_boxscore.Transform.box`
     - Given the extracted box data, transforms data to dictionaries matching the format of nine tables in SQL db

    # Load
     - Calls *initiate_insert()* which executes the SQL upsert process, but just returns transformed data.
     - Upserts to **Team, Arena, Official, Player, Game, GameExt, TeamBox, PlayerBox, and StartingLineups**

    # Downstream Pipelines
     - :class:`~pipelines.playbyplay.PlayByPlayPipeline`

    '''

    def __init__(self, pipeline_name: str, sc_data: dict, environment: str = 'Production'):
        '''`init`(pipeline_name: *str*, sc_data: *dict*, environment: *str*, )
        ---
        <hr>
        
        Initializes Boxscore pipeline for a particular **game**
        - Inherits :attr:`~base.Pipeline.logger`, :attr:`~base.destination` and :attr:`~base.run_timestamp` from superclass (:class:`~pipelines.base.Pipeline`).
        - Sets :attr:`~Data` equal to Scoreboard/Schedule data for that game.
        - Sets :attr:`~GameID` and :attr:`~GameIDStr`
        - Sets :attr:`~url`, :attr:`~source`, and :attr:`~transformer`
        
        <hr>
        
        Parameters
        ---
        :param (*str*) `pipeline_name`: Name of the pipeline, **boxscore.{GameID}**
        :param (*dict*) `sc_data`: Scoreboard data
        :param (*str*) `environment`: either 'Production' or 'Development'. If the latter, uses the file found in .tests\\box
        
        <hr>
        
        Returns
        ---
        '''
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
        '''`extract`(self)
        ---
        <hr>
        
        Fetches data from NBA's static data feeds
        
        ### Downstream Function Calls 
         #### :meth:`~connectors.static_data.StaticDataConnector.fetch`
            - Handles data extraction. Uses the url from `self.url`
        
        <hr>
        
        Returns
        ---
        :return `data` (*dict*): Dict containing 'meta' and **'game'** dicts
        
        <h4>Example
        >>> data_extract = {
            "meta": {...}, 
            "game": {
                'gameId': '0022501129', 
                'gameTimeLocal': '2026-04-04T15:00:00-04:00', 
                'gameTimeUTC': '2026-04-04T19:00:00Z', 
                'gameTimeHome': '2026-04-04T15:00:00-04:00', 
                'gameTimeAway': '2026-04-04T15:00:00-04:00', 
                'gameEt': '2026-04-04T15:00:00-04:00', 
                'duration': 19, 
                'gameCode': '20260404/WASMIA', 
                'gameStatusText': 'Q1 3:32', 
                'gameStatus': 2, 
                'regulationPeriods': 4, 
                'period': 1, 
                'gameClock': 'PT03M32.00S', 
                'attendance': 0, 
                'sellout': '0', 
                'arena': {...}, 
                'officials': [{...}, {...}, {...}], 
                'homeTeam': {
                    'teamId': 1610612748, 
                    'teamName': 'Heat', 
                    'teamCity': 'Miami', 
                    'teamTricode': 'MIA', 
                    'score': 21, 
                    'inBonus': '0', 
                    'timeoutsRemaining': 6, 
                    'periods': [...], 
                    'players': [...], 
                    'statistics': {...}
                }, 
                'awayTeam': {...}
            }
        }
        '''
        self.logger.info(f'Fetching {self.GameID} Box data from {self.url}')
        data_extract = self.source.fetch() if self.environment == 'Production' else self.source.fetch_file()
        self.logger.info(f'Extracted {self.GameID} Box data')
        return data_extract


    def transform(self, data_extract: dict) -> dict:
        '''`transform`(data_extract: *dict*, )
        ---
        <hr>
        
        Transforms extracted Boxscore and Scoreboard/Schedule data into 9 dicts formatted for SQL.

        Also creates start_action_keys and lineup_keys, neccessary for PlayByPlay
        
        ### Downstream Function Calls 
         #### :meth:`~transforms.transform_boxscore.Transform.box`
            - Transforms boxscore data_extract to format that matches table structure of tables defined in the **sql/tables** folder

        <hr>
        
        Parameters
        ---
        :param (*dict*) `data_extract`: Boxscore data extract
        
        <hr>
        
        Returns
        ---
        :return `data_transformed` (dict): Formatted Box data

        <h4>Example
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
        '''`load`(data_transformed: *dict*, )
        ---
        <hr>
        
        Calls *initiate_insert()* which executes the SQL upsert process, but just returns transformed data.
        
        Upserts to **Team, Arena, Official, Player, Game, GameExt, TeamBox, PlayerBox, and StartingLineups**
            
        <hr>
        
        Parameters
        ---
        :param (*dict*) `data_transformed`: Transformed Boxscore data ready to be inserted to SQL db.
        
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

        <hr>
        
        Returns
        ---
        :return `data_transformed` (dict): Same as data_transformed param
        '''
        data_loaded = self.destination.initiate_insert(data_transformed['sql_tables'])
        return data_transformed