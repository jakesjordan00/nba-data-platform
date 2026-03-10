if __name__ == '__main__':
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import Pipeline
from connectors import SQLConnector
import polars as pl

class ScheduleForAPI(Pipeline):
    def __init__(self):
        self.pipeline_name = 'schedule_for_api'
        self.pipeline_tag = 'schedule'
        super().__init__(self.pipeline_name, self.pipeline_tag, 'SQL')
        self.source = self.destination
        
    def extract(self):
        data_extract = self.source.query_to_dataframe(self.source.queries.schedule_for_api_usage)
        return {'data_extract': data_extract}
    

    def transform(self, data_extract):
        data_extract = pl.DataFrame(data_extract['data_extract'])
        db_schedule = data_extract.with_columns([
            (pl.col('GameTimeEST').dt.strftime('%m/%d/%Y')).alias('Date')
        ])
        distinct_dates = db_schedule.sql('select distinct Date from self').to_series().to_list()
        data_transformed = {
            'db_schedule': db_schedule,
            'dates': distinct_dates
        }
        return data_transformed

    def load(self, data_transformed):
        return data_transformed




if __name__ == '__main__':
    pipe = ScheduleForAPI()
    go = pipe.run()
    bp = 'here'