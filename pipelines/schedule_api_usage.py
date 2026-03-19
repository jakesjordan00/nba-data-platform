if __name__ == '__main__':
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import Pipeline
from connectors.sql import SQLConnector, Query
import polars as pl

class ScheduleForAPI(Pipeline):
    def __init__(self, schema: str | None = None, tracking_measure: str | None = None):
        self.pipeline_name = 'schedule_for_api'
        self.pipeline_tag = 'schedule'
        self.schema = schema
        self.tracking_measure = tracking_measure
        super().__init__(self.pipeline_name, self.pipeline_tag, 'SQL')
        self.source = self.destination
        
    def extract(self):
        if not self.tracking_measure:
            schema_query = self.source.queries.schedule_api_playerbox.query.format(schema=self.schema, table='PlayerBox')
        else:
            schema_query = self.source.queries.schedule_api_playerbox.query.format(schema='tracking', table=f'Player{self.tracking_measure}')
        query = Query(name=f'{self.pipeline_name}_{self.schema}', query = schema_query)
        data_extract = self.source.query_to_dataframe(query=query)
        return {'data_extract': data_extract}
    

    def transform(self, data_extract):
        data_extract = pl.DataFrame(data_extract['data_extract'])
        db_schedule = data_extract.with_columns([
            (pl.col('GameTimeEST').dt.strftime('%m/%d/%Y')).alias('Date')
        ])
        #TODO
        db_schedule = db_schedule.group_by([
            'SeasonID',
            'GameID',
            'GameTimeEST',
            'Date',
            'HomeID',
            'AwayID'
        ]).agg([
            pl.col('PlayerID').filter(pl.col('HomeAway') == 'Home').alias('home_players'),
            pl.col('PlayerID').filter(pl.col('HomeAway') == 'Away').alias('away_players')  
        ]).sort(['GameTimeEST', 'GameID'])
        distinct_dates = db_schedule.sql('select distinct Date from self').to_series().to_list()

        dates_with_game_data = []

        for date in distinct_dates:
            games = db_schedule.sql(f"select * from self where Date = '{date}'").to_dicts()
            dates_with_game_data.append({
                'date': date,
                'games': games
            })
        data_transformed = dates_with_game_data
        return data_transformed

    def load(self, data_transformed):
        return data_transformed




if __name__ == '__main__':
    pipe = ScheduleForAPI('adv')
    go = pipe.run()
    bp = 'here'