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


    def extract(self) -> dict:
        '''`extract`(self)
        ---
        <hr>

        Fetches Scoreboard data from [NBA's static data feed](https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json)
        
        ### Downstream Function Calls 
         #### :meth:`~connectors.static_data.StaticDataConnector.fetch`
            - Handles data extraction. Uses the url from `self.url`

        <hr>
        
        :return data_extract (dict): Dict containing 'meta' and **'scoreboard'** dicts

        Example
        ------------
        >>> data_extract = {
            'meta': {
                'version': 1, 
                'request': 'https://nba-prod-us-east-1-mediaops-stats.s3.amazonaws.com/NBA/liveData/scoreboard/todaysScoreboard_00.json', 
                'time': '2026-04-04 03:14:06.146', 
                'code': 200
            },
            'scoreboard': {
                'gameDate': '2026-04-04', 
                'leagueId': '00', 'leagueName': 'National Basketball Association', 
                'games': [
                    {'gameId': '0022501129', 'gameCode': '20260404/WASMIA', 'gameStatus': 2, 'gameStatusText': 'Q1 10:47', 'period': 1, 'gameClock': 'PT10M47.00S', 'gameTimeUTC': '2026-04-04T19:00:00Z', 'gameEt': '2026-04-04T15:00:00-04:00', 'regulationPeriods': 4, 'ifNecessary': False, 'seriesGameNumber': '', 'gameLabel': '', 'gameSubLabel': '', 'seriesText': '', 'seriesConference': '', 'poRoundDesc': '', 'gameSubtype': '', 'isNeutral': False, 'homeTeam': {'teamId': 1610612748, 'teamName': 'Heat', 'teamCity': 'Miami', 'teamTricode': 'MIA', 'wins': 40, 'losses': 37, 'score': 5, 'seed': None, 'inBonus': '0', 'timeoutsRemaining': 7, 'periods': [...]}, 'awayTeam': {'teamId': 1610612764, 'teamName': 'Wizards', 'teamCity': 'Washington', 'teamTricode': 'WAS', 'wins': 17, 'losses': 59, 'score': 3, 'seed': None, 'inBonus': '0', 'timeoutsRemaining': 7, 'periods': [...]}, 'gameLeaders': {'homeLeaders': {...}, 'awayLeaders': {...}}, 'pbOdds': {'team': None, 'odds': 0.0, 'suspended': 0}}, 
                    {...}, 
                    {...}
                ]
            }
        }
        '''
        data_extract = self.source.fetch() if self.environment == 'Production' else self.source.fetch_file()
        try:
            scoreboard = data_extract['scoreboard']
            self.logger.info(f'Extracted Scoreboard for {scoreboard['gameDate']} - {len(scoreboard['games'])} games')
        except Exception as e:
            self.logger.error(f'Scoreboard not found!')
        return data_extract


    def transform(self, data_extract: dict) -> list:
        '''`transform`(self, data_extract: *dict*)
        ---

        Returns a list of formatted Scoreboard dictionaries

        ### Downstream Function Calls 
         #### :meth:`~transforms.transform_data.Transform.scoreboard`
            - Handles transformation of scoreboard data

        <hr>
        
        Parameters
        ---
        :param (*dict*) `data_extract`: Output of fetch()/extract(). Contains game information for today's games
        
        <hr>
        
        Returns
        ---
        :return `data_transformed` (list): List of games taking place today that are **In progress** or **Completed**
        
        <h4>Example
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