from airflow.sdk import dag, task, TaskGroup
from datetime import datetime, timedelta
DOCUMENTATION = """# NBA Hustle Stats Pipeline
"""

def build_pipeline_with_parameters(dag_id, display_name, schedule, where_addition = '', schedule_tag = 'daily', **kwargs):
    @dag(
        dag_id = dag_id,
        dag_display_name = display_name,
        schedule = schedule,
        doc_md = DOCUMENTATION,
        tags = [
            schedule_tag,    
            'src - nba api',
            'statistics',
            'hustle',
            'schema - tracking'  
        ],
        **kwargs
    )
    def nba_hustle_pipeline():
        from pipelines import LeagueDashAPI, ScheduleForAPI
        pipeline_nba_api = LeagueDashAPI()
        data_schedule_for_api = ScheduleForAPI()
        for pt in ['Player','Team']:
            tg_id = f'{pt.lower()}_hustle'
            tg_name = f'{pt} Hustle - tracking.{pt}Hustle'
            with TaskGroup(group_id=tg_id,group_display_name=tg_name) as taskgroup:
                t_id = f'schedule_{pt.lower()}_hustle'
                t_name = f'Schedule - {pt} Hustle'
                @task(task_id=t_id,task_display_name=t_name)
                def get_schedule(pt = pt):
                    data_schedule_for_api._re_init(
                        schema='tracking', 
                        table_base_name='Hustle',
                        player_team = pt,
                        log_tag = f'.{pt}_hustle'.lower(),
                        where_addition = 'and s.GameTimeEST >= cast(getdate()-2 as date)'
                    )
                    completed_schedule_pipeline = data_schedule_for_api.run()
                    schedule_data = completed_schedule_pipeline['transformed']['data_transformed']
                    return schedule_data
                
                t_id = f'league_dash_{pt.lower()}_hustle'
                t_name = f'Leaguedash API - {pt} Hustle'
                @task(task_id=t_id,task_display_name=t_name)
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


build_pipeline_with_parameters(
    dag_id = 'league_dash_hustle_pipeline_hourly',
    display_name = 'NBA API - Hourly Hustle Stats Pipeline',
    schedule = '5 16-23,0-8 * * *',
    schedule_tag = 'hourly',
    start_date = datetime(2025, 3, 1),
    catchup = False,
    max_active_runs = 1,
    max_active_tasks= 4,
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(seconds=30)
    }

)

build_pipeline_with_parameters(
    dag_id = 'league_dash_hustle_pipeline_daily',
    display_name = 'NBA API - Daily Hustle Stats Pipeline',
    schedule = '45 18 * * *', #18 = 2pm EST -> 2:45pm EST
    schedule_tag = 'daily',
    start_date = datetime(2025, 3, 1),
    catchup = False,
    max_active_runs = 1,
    max_active_tasks= 4,
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(seconds=30)
    }

)