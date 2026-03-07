import logging
import requests
import os
import json

class StaticDataConnector:
    '''StaticDataConnector
===
Used to extract data from NBA static data feeds

## Functions
    ### ```def __init__```(self, pipeline):
        - Initializes Connector. 
        - Sets self.pipeline and self.logger
            - Passing pipeline enables:
                - Usage of **pipeline.url** for the link to the data feed
                - More granular logging
        
    ### ```def fetch```(self):
        - Fetches data from the NBA static data feed using the pipeline's url
        - Used for **Scoreboard**, **Schedule**, **Boxscore** and **PlayByPlay** (and more)

    Attributes: 
        schedule: *NBA static data feed for a season's Schedule*<br>&emsp;- https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_2.json
        
        scoreboard: *NBA static data feed for today's scoreboard*<br>&emsp;-  https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json
        
        playbyplay: *NBA static data feed for PlayByPlay*<br>&emsp;- Must use ***.replace('GameIDStr', self.GameIDStr)***<br>&emsp;- Example: https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_0022500356.json
        
        boxscore: *NBA static data feed for Boxscore* <br>&emsp;- Must use ***.replace('GameIDStr', self.GameIDStr)*** <br>&emsp;- Example: https://cdn.nba.com/static/json/liveData/boxscore/boxscore_0022500356.json
        
        daily_lineups: *NBA static data feed for Team Lineups on a given day*<br>&emsp;- Must use ***.replace('YYYYmmdd', date)***<br>&emsp;- Example: https://stats.nba.com/js/data/leaders/00_daily_lineups_20260306.json
    '''
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.extract')
        self._set_base_data_feeds()
        self._set_variable_data_feeds()
        pass
    
    def fetch(self) -> dict:
        '''Summary
        -------------
        Fetches data from NBA's static data feeds
                
        :return data (dict): Dict containing subdictionaries. Usually a 'meta' dict and then the dict that contains our data

            - 'meta', 'scoreboard'
            - 'meta', 'leagueSchedule'
            - 'meta', 'game'
            

        Examples
        ------------
        >>> {"meta": {}, "scoreboard": {}}
        >>> {"meta": {}, "leagueSchedule": {}}
        >>> {"meta": {}, "game":{}}
        '''
        try:
            response = requests.get(self.pipeline.url)
            data = response.json()
        except Exception as e:
            data = {}
            self.logger.error(f'No data available. Error msg: {e}')
        self.logger.info(f'Extracted {', '.join(key.replace("'", "") for key in list(data.keys()))} dicts')
        return data
    
    def fetch_file(self):
        dir_files = os.listdir(self.pipeline.file_source)
        test = f'{self.pipeline.file_source}/{dir_files[0]}'
        with open(f'{self.pipeline.file_source}/{dir_files[0]}', 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    



    def _set_base_data_feeds(self):
        self.schedule = 'https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_2.json'
        self.scoreboard = 'https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json'
        self.player_movement = 'https://stats.nba.com/js/data/playermovement/NBA_Player_Movement.json'
        self.nightly_dunk_score = 'https://cdn.nba.com/static/json/staticData/leaderboards/00_nightlydunkscore.json'
    
    def _set_variable_data_feeds(self):
        self.daily_lineups = 'https://stats.nba.com/js/data/leaders/00_daily_lineups_YYYYmmdd.json'
        self.boxscore = 'https://cdn.nba.com/static/json/liveData/boxscore/boxscore_GameIDStr.json'
        self.playbyplay = 'https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_GameIDStr.json'