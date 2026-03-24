

from airflow.sdk import dag, task, BaseHook, TaskGroup
from datetime import datetime, timedelta
import textwrap

@dag(
    dag_id = 'league_dash_advanced_metrics_pipeline',
    dag_display_name = 'NBA Advanced Metrics Pipeline',
    start_date = datetime(year=2026, month=3, day=10),
    schedule = '0 12-23,0-4/1 * * *',
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(seconds=30)
    },
    doc_md = textwrap.dedent(
    text="""
"""
    )
)
def nba_advanced_metrics_pipeline():
    from pipelines import LeagueDashAPI, ScheduleForAPI
    pipeline_nba_api = LeagueDashAPI()
    data_schedule_for_api = ScheduleForAPI()
    for pt in ['Player','Team']:
        for schema, measure_type in {
            'adv': 'Advanced',
            'misc': 'Misc',
            'def': 'Defense',
            'violations': 'Violations',
            'usage': 'Usage',                   #No Usage for Team
            'ffactors': 'Four Factors',         #No usage for Player
        }.items():
            if (pt == 'Team' and measure_type == 'Usage') or (pt == 'Player' and measure_type == 'Four Factors'):
                continue

            with TaskGroup(group_id = f'{pt.lower()}_advanced_metrics_{schema}',group_display_name = f'{pt} Advanced Metrics - {measure_type}') as taskgroup:
                
                @task(
                    task_id = f'{pt.lower()}_{schema}_schedule',
                    task_display_name = f'Schedule - {pt} {measure_type}'
                )
                def get_schedule(schema = schema, pt = pt, measure_type = measure_type):
                    data_schedule_for_api._re_init(
                        schema = schema,
                        table_base_name = 'Box',
                        player_team = pt,
                        log_tag = f'.{pt.lower()}',
                        )
                    schedule_pipeline = data_schedule_for_api.run()
                    schedule_data = schedule_pipeline['loaded']
                    return schedule_data

                @task(
                    task_id = f'league_dash_{pt.lower()}_{schema}',
                    task_display_name = f'Leaguedash API - {pt} {measure_type}'
                )
                def get_measure_type_data(date, schema = schema, pt = pt, measure_type = measure_type):
                    pipeline_nba_api._re_init(
                        schema=schema,
                        params = {
                            'MeasureType': measure_type,
                        },
                        endpoint_friendly_name = f'{pt}_stats'.lower(),
                        table_base_name = 'Box',
                        player_team = pt,
                        log_tag = f'.{pt.lower()}',
                        extract_tag = f'Extracting {pt} {measure_type} data from {date['date']} via the NBA API'
                    )
                    measure_type_pipeline = pipeline_nba_api.run(date_data = date)
                    measure_type_data = measure_type_pipeline['loaded']


                schedule_dates_to_do = get_schedule()

                get_measure_type_data.expand(date = schedule_dates_to_do)


nba_advanced_metrics_pipeline()