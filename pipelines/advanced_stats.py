from pipelines import Pipeline
from connectors import StaticDataConnector, APIDataConnector
from transforms.transform_api_data import Transform
from datetime import datetime
import polars as pl

class AdvancedStatsPipeline(Pipeline):
    def __init__(self, data: list | None):
        self.pipeline_name = 'advanced_stats'
        self.tag = 'advancedStats'
        super().__init__(self.pipeline_name, self.tag, 'NBA API')
        self.schedule_source = StaticDataConnector(self)
        self.url = self.schedule_source.schedule        
        self.source = APIDataConnector(self)
        self.data = data
        self.transformer = Transform(self)
        # self.destination.check_tables()

    def extract(self):
        data_extract = self.source.fetch(self.source.player_stats)
        return data_extract
    

    def transform(self, data_extract):
        data_transformed = self.transformer.start_transform(data_extract)
        return data_transformed

    def load(self, data_transformed):
        data_loaded = data_transformed
        for item in data_transformed[0].keys():
            print(f"            '{item}',")
        bp = 'here'



