from pipelines import Pipeline
from connectors import StaticDataConnector, APIDataConnector
from transforms.transform_api_data import Transform
from datetime import datetime
import polars as pl



class AdvancedStatsPipeline(Pipeline):
    def __init__(self, schema: str, params: dict,  endpoint_friendly_name: str, tracking_table: str | None = None, player_team: str | None = None, log_tag: str | None= None):
        self.pipeline_name = f'advanced_stats.{schema}{log_tag}'
        self.tag = 'advancedStats'
        self.schema = schema
        self.tracking_table = tracking_table
        self.full_table_name = f'{player_team}{tracking_table}'
        self.player_team = player_team
        self.params = params
        super().__init__(self.pipeline_name, self.tag, 'NBA API')
        self.schedule_source = StaticDataConnector(self)
        self.url = self.schedule_source.schedule        
        self.source = APIDataConnector(self)
        
        self._endpoint = self.source.get_endpoint(friendly_name=endpoint_friendly_name)
        if not tracking_table:
            self.full_table_name = 'PlayerBox'
            # self._endpoint = self.source.player_stats
        # elif tracking_table and tracking_table != 'Hustle':
            # self._endpoint = self.source.pt_tracking
        # elif tracking_table and self.full_table_name == 'PlayerHustle':
            # self._endpoint = self.source.player_hustle
        # elif tracking_table and self.full_table_name == 'TeamHustle':
            # self._endpoint = self.source.team_hustle

        self._params = {
            **self._endpoint.params,
            **params
        }
        try:
            self.destination.check_specific_table(f'{self.schema}.{self.full_table_name}')
        except Exception as e:
            test = e
            self.logger.critical(f"Table doesn't exist in config/settings.py! Continuing to allow for debugging, but nothing will be inserted.")
            bp = 'here'
        self.runs = 0


    def extract(self):
        self.logger.info(f'Extracting data from {self.date}')
        data_extract = self.source.fetch(endpoint=self._endpoint, params = self._params)
        return data_extract
    

    def transform(self, data_extract):
        if not data_extract:
            self.logger.warning('No data extracted, skipping transform')
            return None
        data_transformed = self.transformer.start_transform(data_extract)
        return data_transformed

    def load(self, data_transformed):
        if not data_transformed:
            self.logger.warning('No data transformed, skipping load')
            return None
        data_loaded = self.destination.checked_upsert(table_name=f'{self.schema}.{self.full_table_name}', data=data_transformed)
        self.runs += 1
        return data_loaded

    def run(self, date_data: dict) -> dict:
        if self.schema != 'plays':
            self.date = date_data['date']
            self.data = date_data['games']
            self._params = {**self._params, 'DateFrom': self.date, 'DateTo': self.date}
        else:
            #If we're doing Playtypes and this isn't our first run, use new params. If it's our first run, use what we passed at init
            if self.runs > 0: 
                self._params = self._endpoint.params
            self.date = None
            self.data = None
        self.transformer = Transform(self)
        return super().run()


