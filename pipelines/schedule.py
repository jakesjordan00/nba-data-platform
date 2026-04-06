from pipelines import Pipeline
from connectors import StaticDataConnector, SQLConnector
from transforms.transform_data import Transform

class SchedulePipeline(Pipeline[list]):

    def __init__(self):
        super().__init__(pipeline_name='schedule', pipeline_tag='leagueSchedule',source_tag='NBA static data feed')
        self.tag = 'leagueSchedule'
        self.source = StaticDataConnector(self)
        self.url = self.source.schedule
        self.transformer = Transform(self)
        self.environment = 'Production'
        self.logger.info('Creating tables if they do not already exist')
        self.destination.check_tables()

    
    def extract(self):
        '''`extract`()
        ---
        <hr>
        
        Fetches Schedule data from NBA's static data feed 
        - 'meta', 'leagueSchedule'
        
        ### Downstream Function Calls 
         #### :meth:`~connectors.static_data.StaticDataConnector.fetch`
            - Handles data extraction
         
        
        <hr>
        
        Returns
        ---
        :return `data_extract` (*dict*): Data extracted from the scheduleLeagueV2_1 data feed
        '''
        data_extract = self.source.fetch()
        return data_extract


    def transform(self, data_extract):
        data_transformed = self.transformer.schedule(data_extract)
        data_db = self.transformer.schedule_db(data_extract)
        data = {
            'data_transformed': data_transformed,
            'data_db': data_db
        }
        return data

    def load(self, data: dict):
        data_loaded = data['data_transformed']
        data_db_loaded = self.destination.checked_upsert('Schedule', data['data_db'])
        return data_loaded


class DailyBackfillSchedulePipeline(SchedulePipeline):
    def __init__(self):
        super().__init__()

    def transform(self, data_extract):
        db_schedule = self.destination.query_to_dataframe(self.destination.queries.schedule_backfill)
        data_extract_formatted = self.transformer.schedule(data_extract)
        data_transformed = self.transformer.schedule_backfill(data_extract_formatted, db_schedule)
        data_db = self.transformer.schedule_db(data_extract)
        data = {
            'data_transformed': data_transformed,
            'data_db': data_db
        }
        return data