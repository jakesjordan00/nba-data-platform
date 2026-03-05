from airflow.sdk import dag, task, BaseHook
from datetime import datetime, timedelta
from dataclasses import asdict

@dag(
    dag_id = 'nba_schedule_pipeline',
    dag_display_name= 'NBA Schedule Games Pipeline',
    start_date = datetime(year=2026, month = 1, day = 1),
    schedule = '0 12 1 * *', #Fire at noon on the first of each month
    catchup = False,
    max_active_runs = 1,
    default_args= {
        'retries': 1,
        'retry_delay': timedelta(seconds = 30)
    }
)

def nba_pipeline():
    @task
    def run_schedule():
        from pipelines import SchedulePipeline
        schedule_pipeline = SchedulePipeline()
        completed_schedule_pipeline = schedule_pipeline.run()
        schedule_data = completed_schedule_pipeline['loaded']
        return schedule_data
    


    def run_boxscore(game):
        from pipelines import BoxscorePipeline
        GameID = game['GameID']
        boxscore_pipeline = BoxscorePipeline(
            pipeline_name = f'boxscore.{GameID}',
            sc_data = game,
            environment = 'Production'
        )
        completed_boxscore_pipeline = boxscore_pipeline.run()
        boxscore_data = completed_boxscore_pipeline['loaded']


        start_action_info = boxscore_pipeline.destination.cursor_query(
            table_name = 'PlayByPlay',
            keys = boxscore_data['start_action_keys']
        )
        db_actions = start_action_info['actions']
        db_last_action_number = start_action_info['last_action_number']
        stint_status = start_action_info['stint_status']
        
        home_stats, away_stats = (None, None) if db_actions == 0 or stint_status == 'failure' else boxscore_pipeline.destination.stint_cursor(boxscore_data['lineup_keys'])




    games = run_schedule()

nba_pipeline()