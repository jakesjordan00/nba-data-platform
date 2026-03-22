from pipelines import Pipeline
from connectors import StaticDataConnector, APIDataConnector
from transforms.transform_api_data import Transform
from datetime import datetime
import polars as pl



class LeagueDashAPI(Pipeline):
    def __init__(self, ):
        self.pipeline_name = f'nba-api'
        self.tag = 'nba-api'
        super().__init__(self.pipeline_name, self.tag, 'NBA API')
        self.source = APIDataConnector(self)
        self.total_runs = 0
        # self.schedule_source = StaticDataConnector(self)
        # self.url = self.schedule_source.schedule


    def extract(self):
        self.logger.info(self.extract_tag)
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
        self.logger.info(f'Loading data via checked_upsert in sql.py')
        data_loaded = self.destination.checked_upsert(table_name=self.full_table_name, data=data_transformed)
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


    def _re_init(self, schema: str, params: dict,  endpoint_friendly_name: str, table_base_name: str, player_team: str, 
                 log_tag: str | None = None, extract_tag: str | None = None):
        self.pipeline_name = f'nba-api.{schema}{log_tag}'
        self.tag = 'nba-api'
        self.extract_tag = extract_tag
        self.schema = schema
        self.table_base_name = table_base_name
        self.table_name = f'{player_team}{table_base_name}'
        self.full_table_name = f'{schema}.{player_team}{table_base_name}'
        self.player_team = player_team
        self.params = params
        
        self._endpoint = self.source.get_endpoint(friendly_name=endpoint_friendly_name)
        self._params = {
            **self._endpoint.params,
            **params
        }
        self.runs = 0
        try:
            self.destination.check_specific_table(self.full_table_name)
        except Exception as e:
            test = e
            self.logger.critical(f"Table doesn't exist in config/settings.py! Continuing to allow for debugging, but nothing will be inserted.")

        
        bp = 'here'