from pipelines.base import Pipeline
from connectors.static_data import StaticDataConnector
from datetime import datetime
from transforms.transform_daily_lineups import Transform

class DailyLineupsPipeline(Pipeline):
    '''`DailyLineupsPipeline`(Pipeline)
    ---
    <hr>
    
    - Given a date, extracts lineup information from the NBA's static data feed
        - By default, runs today's date
    
    # Extraction
    :meth:`~extract` -> :class:`~connectors.static_data.StaticDataConnector`.:meth:`~connectors.static_data.StaticDataConnector.fetch`
    - Fetches a date's Lineup information for the teams partaking in that date's games

    # Transformation
    :meth:`~transform` -> :class:`~transforms.transform_boxscore.Transform`.:meth:`~transforms.transform_boxscore.Transform.box`
     - Given the extracted daily lineups data, transforms data to a list of dictionaries that are formatted for the **DailyLineups** table in SQL db

    # Load
     - Calls *initiate_insert()* which executes the SQL upsert process, but just returns transformed data.
     - Upserts to **DailyLineups**
    '''
    def __init__(self, pipeline_name: str):
        '''`init`(self, pipeline_name: *str*)
        ---
        <hr>
        
        
        Initializes DailyLineups pipeline for a particular **date**
        - Inherits :attr:`~base.Pipeline.logger`, :attr:`~base.destination` and :attr:`~base.run_timestamp` from superclass (:class:`~pipelines.base.Pipeline`).
        - Sets :attr:`~date` equal to todays date, (yyyy/mm/dd)
        - Sets :attr:`~GameID` and :attr:`~GameIDStr`
        - Sets :attr:`~url`, :attr:`~source`, and :attr:`~transformer`
        
        ### Downstream Calls 
         #### :meth:`~connectors.static_data.StaticDataConnector.check_tables`
            - For each table in TABLES dict, run the create statement associated to create the table if it does not already exist
            
        <hr>
        
        Parameters
        ---
        :param (*str*) `pipeline_name`: _description_
        
        <hr>
        
        Returns
        ---
        '''
        date = datetime.now().date().strftime('%Y%m%d')
        display_date = datetime.now().strftime('%m/%d/%Y')
        display_datetime = datetime.now().strftime('%I:%M%p').lower()
        super().__init__(pipeline_name=pipeline_name, 
                         pipeline_tag=f"{display_date}'s lineups as of {display_datetime}",
                         source_tag='NBA static data feed')
        self.source = StaticDataConnector(self)
        self.transformer = Transform(self)
        self.url = self.source.daily_lineups.replace('YYYYmmdd', date)
        self.destination.check_tables()
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
        data_loaded = self.destination.checked_upsert('DailyLineups', data_transformed)
        return data_loaded



