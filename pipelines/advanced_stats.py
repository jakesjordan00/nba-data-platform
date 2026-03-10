if __name__ == '__main__':
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import Pipeline
from connectors import StaticDataConnector, APIDataConnector
from datetime import datetime
import polars as pl
class AdvancedStatsPipeline(Pipeline):
    def __init__(self):
        self.pipeline_name = 'advanced_stats'
        self.tag = 'advancedStats'
        super().__init__(self.pipeline_name, self.tag, 'NBA API')
        self.schedule_source = StaticDataConnector(self)
        self.url = self.schedule_source.schedule        
        self.source = APIDataConnector(self)
        # self.destination.check_tables()

    def extract(self):
        schedule_extract = self.destination.queries.schedule_for_api_usage
        data_extract = self.source.fetch()
        return data_extract
    

    def transform(self, data_extract):
        data_transformed = data_extract

    def load(self, data_transformed):
        data_loaded = data_transformed



if __name__ == '__main__':
    pipe = AdvancedStatsPipeline()

    db_schedule = pipe.destination.query_to_dataframe(pipe.destination.queries.schedule_for_api_usage)
    db_schedule = db_schedule.with_columns([
        (pl.col('GameTimeEST').dt.strftime('%m/%d/%Y')).alias('Date')
    ])
    distinct_dates = db_schedule.sql('select distinct Date from self')

    params = pipe.source.player_stats.params
    for row in distinct_dates.iter_rows(named=True):
        date = row['Date']
        pipe.source.player_stats.params = {**params, 'DateFrom': date, 'DateTo': date}
        response = pipe.source.fetch(pipe.source.player_stats)
        bp ='here'

    bp = 'here'
