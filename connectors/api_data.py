from urllib import response
import config.api_map as map
import requests
import json


class APIDataConnector:   
    class Endpoint:
        def __init__(self, friendly_name: str, endpoint_name) -> None:
            self.name = friendly_name
            config = map.nba_advanced_stats_endpoints[endpoint_name]
            self.url = config['url']
            self.headers = config['headers']
            self.params = config['params']
            pass

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self._set_endpoints()
        pass    



    def fetch(self, endpoint: Endpoint):
        response = requests.get(url=endpoint.url, params=endpoint.params, headers=endpoint.headers)
        api_extract = response.json()
        return api_extract
    

    def _set_endpoints(self):
        self.player_stats = self.Endpoint('player_stats', 'leaguedashplayerstats')
        self.player_tracking = self.Endpoint('player_tracking_stats', 'leaguedashptstats')