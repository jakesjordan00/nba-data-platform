import polars as pl
import pandas as pd
from pipelines.base import Pipeline
from connectors.static_data import StaticDataConnector
from transforms.transform_data import Transform


class ScoreboardPipeline(Pipeline[list]):
    '''ScoreboardPipeline
===
Fetches Games taking place today from NBA static data feed.
___
* https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json
* Scoreboard resets each day at 12pm EST (Noon). If pulled before then, will show games from last night

## Functions
    ### ```def __init__```(self, environment: str):
        - Initializes Schedule pipeline
        - Inherits logger, destination and run_timestamp from superclass
        - Sets url, tag, source, transformer, environment and file_source

    ### ```extract```(self):
        https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json
        - Fetches Scoreboard data from NBA's static data feed
            - Returns **data_extract**: Dict containing 'meta' and **'scoreboard'** dicts

    ### ```transform```(self, data_extract):
        - Given data_extract, returns a list of formatted Scoreboard dictionaries
            - Parameter: **data_extract**: Output of fetch()/extract(). Contains game information for today's games
            - Returns **data_transformed**: List of games taking place today that are **In progress** or **Completed**

    ### ```load```(self, data_transformed):
        - Does nothing and returns untouched data_transformed parameter

    '''

    def __init__(self, environment: str):
        super().__init__(pipeline_name='scoreboard', pipeline_tag='todaysScoreboard', source_tag='NBA static data feed')

        self.tag = 'todaysScoreboard'
        self.source = StaticDataConnector(self)
        self.url = self.source.scoreboard
        self.transformer = Transform(self)
        self.environment = environment
        self.file_source = '.tests/scoreboard'


    def extract(self):
        '''Summary
        -------------
        Fetches Scoreboard data from NBA's static data feed

        :return data_extract (dict): Dict containing 'meta' and **'scoreboard'** dicts

    Example
        ------------
        >>> {"meta": {},"scoreboard": {}}
        '''
        data_extract = self.source.fetch() if self.environment == 'Production' else self.source.fetch_file()
        try:
            scoreboard = data_extract['scoreboard']
            self.logger.info(f'Extracted Scoreboard for {scoreboard['gameDate']} - {len(scoreboard['games'])} games')
        except Exception as e:
            self.logger.error(f'Scoreboard not found!')
        return data_extract


    def transform(self, data_extract):
        '''Summary
        -------------
        Returns a list of formatted Scoreboard dictionaries

        :param data_extract: Output of fetch()/extract(). Contains game information for today's games
        :type data: dict
        :return data_transformed: List of games taking place today that are **In progress** or **Completed**
        :rtype: list
        
        Example
        ------------
        >>> [{'SeasonID': 2025, 'GameID': 22500831, 'GameIDStr': '0022500831', 'GameCode': '20260224/PHIIND', 'GameStatus': 3, 'GameStatusText': 'Final', 'Period': 4, 'GameClock': '', 'GameTimeUTC': '2026-02-25T00:00:00Z', 'GameEt': '2026-02-24T19:00:00Z', 'RegulationPeriods': 4, 'IfNecessary': False, 'SeriesGameNumber': '', 'GameLabel': '', 'GameSubLabel': '', 'SeriesText': '', 'SeriesConference': '', 'RoundDesc': '', 'GameSubtype': '', 'IsNeutral': False, 'HomeTeam': {'teamId': 1610612754, 'teamName': 'Pacers', 'teamCity': 'Indiana', 'teamTricode': 'IND', 'wins': 15, 'losses': 44, 'score': 114, 'seed': None, 'inBonus': None, 'timeoutsRemaining': 0, 'periods': [...]}, 'AwayTeam': {'teamId': 1610612755, 'teamName': '76ers', 'teamCity': 'Philadelphia', 'teamTricode': 'PHI', 'wins': 32, 'losses': 26, 'score': 135, 'seed': None, 'inBonus': None, 'timeoutsRemaining': 1, 'periods': [...]}},]
        '''
        data_transformed = self.transformer.scoreboard(data_extract)
        return data_transformed



    def load(self, data_transformed):
        '''Summary
        -------------
        Don't load scoreboard data
        '''
        return data_transformed