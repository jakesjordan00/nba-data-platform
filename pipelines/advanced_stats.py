from pipelines import Pipeline
from connectors import StaticDataConnector, APIDataConnector
from transforms.transform_api_data import Transform
from datetime import datetime
import polars as pl



TRACKING_MEASURE_CONFIG ={
    '', ''
}


class AdvancedStatsPipeline(Pipeline):
    def __init__(self, schema: str, params: dict, tracking_table: str | None = None):
        self.pipeline_name = f'advanced_stats.{schema}'
        self.tag = 'advancedStats'
        self.schema = schema
        self.params = params
        # self.tracking_measure = tracking_measure
        super().__init__(self.pipeline_name, self.tag, 'NBA API')
        self.schedule_source = StaticDataConnector(self)
        self.url = self.schedule_source.schedule        
        self.source = APIDataConnector(self)
        
        if not tracking_table:
            self._endpoint = self.source.player_stats
        elif tracking_table:
            self._endpoint = self.source.player_tracking
        self._params = {
            **self._endpoint.params,
            **params
        }


        # self.destination.check_tables()

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
        data_loaded = self.destination.checked_upsert(table_name=f'{self.schema}.PlayerBox', data=data_transformed)
        return data_loaded

    def run(self, date_data: dict) -> dict:
        self.date = date_data['date']
        self.data = date_data['games']
        self._params = {**self._params, 'DateFrom': self.date, 'DateTo': self.date}
        self.transformer = Transform(self)
        return super().run()


