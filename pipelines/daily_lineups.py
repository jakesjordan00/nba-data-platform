from pipelines.base import Pipeline
from connectors.static_data import StaticDataConnector
from datetime import datetime
from transforms.transform_daily_lineups import Transform

class DailyLineupsPipeline(Pipeline):
    def __init__(self, pipeline_name: str):
        date = datetime.now().date().strftime('%Y%m%d')
        display_date = datetime.now().strftime('%m/%d/%Y')
        display_datetime = datetime.now().strftime('%I:%M%p').lower()
        super().__init__(pipeline_name=pipeline_name, 
                         pipeline_tag=f"{display_date}'s lineups as of {display_datetime}",
                         source_tag='NBA static data feed')
        self.source = StaticDataConnector(self)
        self.transformer = Transform(self)
        self.url = self.source.daily_lineups.replace('YYYYmmdd', date)
        bp = 'here'

    def extract(self):
        data_extract = self.source.fetch()
        try:
            data_extract = data_extract['games']
        except Exception as e:
            self.logger.error('Daily Lineups not found!')
        return data_extract
    
    def transform(self, data_extract: list):
        data_transformed = self.transformer.daily_lineups(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        data_loaded = data_transformed
        return data_loaded



