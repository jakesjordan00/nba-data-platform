from urllib import response
import config.api_map as map
import requests
import json
import time
import logging
from datetime import datetime


class APIDataConnector:   
    class Endpoint:
        def __init__(self, friendly_name: str, endpoint_name) -> None:
            self.name = friendly_name
            config = map.nba_api_endpoints[endpoint_name]
            self.url = config['url']
            self.headers = config['headers']                
            self.params = config['params']
            pass

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.api')
        self._set_endpoints()
        pass    



    def fetch(self, endpoint: Endpoint, params: dict = None, retries=2, backoff=5):
        if params:
            endpoint.params = params
        for attempt in range(retries):
            response = requests.get(url=endpoint.url, params=endpoint.params, headers=endpoint.headers)
            if response.status_code == 500:
                if '\n' in response.text:
                    text = response.text.split('\n')[0]
                else:
                    text = 'ERROR'
                self.logger.info(f'{response.status_code}: {text}')
                if attempt < retries:
                    self.logger.warning(f'{response.status_code} ERROR on try {attempt}: Waiting {backoff * attempt} seconds...')
                    time.sleep(backoff * attempt)
                    continue
                self.logger.warning(f'{response.status_code}: {response.reason}')
            api_extract = response.json()
            return api_extract
    

    def get_endpoint(self, friendly_name: str) -> Endpoint:
        endpoint_name = map.friendly_name_map[friendly_name.lower()]
        return self.Endpoint(friendly_name, endpoint_name)

    def _set_endpoints(self):
        self.player_stats   = self.Endpoint(
            friendly_name='player_stats', 
            endpoint_name='leaguedashplayerstats'
        )
        self.pt_tracking    = self.Endpoint(
            friendly_name='player_tracking_stats',
            endpoint_name='leaguedashptstats'
        )
        self.player_hustle  = self.Endpoint(
            friendly_name='player_hustle',
            endpoint_name='leaguehustlestatsplayer'
        )
        self.team_hustle    = self.Endpoint(
            friendly_name = 'team_hustle',
            endpoint_name='leaguehustlestatsteam'
        )
        self.pt_play_type   = self.Endpoint(
            friendly_name = 'pt_play_type',
            endpoint_name = 'synergyplaytypes'
        )