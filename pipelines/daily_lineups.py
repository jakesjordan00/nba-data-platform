from pipelines.base import Pipeline
from connectors.static_data import StaticDataConnector
from datetime import datetime

class DailyLineupsPipeline(Pipeline):
    def __init__(self, pipeline_name: str):
        super().__init__(pipeline_name=pipeline_name, 
                         pipeline_tag='lineups', 
                         source_tag='NBA static data feed')
        self.source = StaticDataConnector(self)
        date = datetime.now().date().strftime('%Y%m%d')
        self.url = self.source.daily_lineups.replace('YYYYmmdd', date)
    bp = 'here'

    def extract(self):
        data_extract = self.source.fetch()
        return data_extract
    
    def transform(self, data_extract: list):
        data_transformed = data_extract
        return data_transformed
    
    def load(self, data_transformed):
        data_loaded = data_transformed
        return data_loaded



test = DailyLineupsPipeline('test')
a = test.run()
bp = 'here'