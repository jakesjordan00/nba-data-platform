from airflow.sdk import dag, task, TaskGroup
from datetime import datetime, timedelta


@dag(
    dag_id = 'league_dash_hustle_pipeline',
    dag_display_name = 'NBA API - Hustle Stats Pipeline',
    start_date = datetime(2025, 3, 1),
    schedule = '5 12-23,0-4/1 * * *',
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(seconds=30)
    },
    tags = [
        'src - nba api',
        'second spectrum',
        'statistics',
        'daily',
        'hustle',        
    ],
    doc_md = """
"""

)
def nba_hustle_pipeline():
    from pipelines import LeagueDashAPI, ScheduleForAPI
    pipeline_nba_api = LeagueDashAPI()
    data_schedule_for_api = ScheduleForAPI()
    for pt in ['Player','Team']:
        with TaskGroup(
            group_id = f'{pt.lower()}_hustle', 
            group_display_name = f'{pt} Hustle - tracking.{pt}Hustle'
        ) as taskgroup:
            @task(
                    task_id=f'schedule_{pt.lower()}_hustle',
                    task_display_name=f'Schedule - {pt} Hustle'
            )
            def get_schedule(pt = pt):
                data_schedule_for_api._re_init(
                    schema='tracking', 
                    table_base_name='Hustle',
                    player_team = pt,
                    log_tag = f'.{pt}_hustle'.lower()
                )
                completed_schedule_pipeline = data_schedule_for_api.run()
                schedule_data = completed_schedule_pipeline['loaded']
                return schedule_data
            
            @task(
                    task_id=f'league_dash_{pt.lower()}_hustle',
                    task_display_name=f'Leaguedash API - {pt} Hustle'
            )
            def get_hustle_data(date, pt = pt):
                pipeline_nba_api._re_init(
                schema = 'tracking',
                params = {},
                endpoint_friendly_name = f'{pt}_hustle'.lower(),
                table_base_name = 'Hustle',
                player_team = pt,
                log_tag = f'.{pt}_hustle'.lower(),
                extract_tag = f'Extracting {pt} Hustle data from {date['date']} via the NBA API'
                )
                hustle_pipeline = pipeline_nba_api.run(date_data=date)
                hustle_metadata = hustle_pipeline['xcom']
                return hustle_metadata

            schedule_dates_to_do = get_schedule()

            hustle_metadata = get_hustle_data.expand(date = schedule_dates_to_do)

nba_hustle_pipeline()