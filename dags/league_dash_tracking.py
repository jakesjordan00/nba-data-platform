from airflow.sdk import dag, task, TaskGroup
from datetime import datetime, timedelta


@dag(
    dag_id = 'league_dash_tracking_pipeline',
    dag_display_name = 'NBA API - SecondSpectrum Tracking Pipeline',
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
        'tracking',        
    ],
    doc_md = """
"""

)
def nba_tracking_pipeline():
    from pipelines import LeagueDashAPI, ScheduleForAPI
    pipeline_nba_api = LeagueDashAPI()
    data_schedule_for_api = ScheduleForAPI()
    PtMeasureType_options = ['Drives', 'Defense', 'CatchShoot', 'Passing', 'Possessions', 'PullUpShot', 'Rebounding', 'Efficiency', 'SpeedDistance', 'ElbowTouch', 'PostTouch', 'PaintTouch']
    
    for pt in ['Player', 'Team']:
        for tracking_measure in PtMeasureType_options:

            with TaskGroup(
                group_id = f'{pt.lower()}_tracking_{tracking_measure.lower()}', 
                group_display_name = f'{pt} {tracking_measure} - tracking.{pt}{tracking_measure}'
            ) as taskgroup:
                
                @task(
                    task_id=f'schedule_{pt.lower()}_tracking',
                    task_display_name=f'Schedule - {pt} {tracking_measure}'
                )
                def get_schedule(pt = pt, tracking_measure = tracking_measure):
                    data_schedule_for_api._re_init(
                        schema = 'tracking', 
                        table_base_name = tracking_measure,
                        player_team = pt,
                        log_tag = f'.{pt}_{tracking_measure}'.lower(),
                    )
                    completed_schedule_pipeline = data_schedule_for_api.run()
                    schedule_data = completed_schedule_pipeline['loaded']
                    return schedule_data
                

                @task(
                    task_id=f'second_spectrum_tracking_{pt.lower()}_{tracking_measure}',
                    task_display_name=f'SecondSpectrum Tracking - {pt} {tracking_measure}'
                )
                def get_tracking_data(date, pt = pt, tracking_measure = tracking_measure):
                    pipeline_nba_api._re_init(
                        schema = 'tracking',
                        params = {
                            'PlayerOrTeam': pt,
                            'PtMeasureType': tracking_measure,
                        },
                        endpoint_friendly_name = 'pt_tracking',
                        table_base_name = tracking_measure,
                        player_team = pt,
                        log_tag = f'.{pt}_{tracking_measure}'.lower(),
                        extract_tag = f'Extracting {pt} {tracking_measure} data from {date['date']} via the NBA/SecondSpectrum API'
                    )
                    tracking_pipeline = pipeline_nba_api.run(date_data=date)
                    tracking_metadata = tracking_pipeline['xcom']
                    return tracking_metadata


                schedule_dates_to_do = get_schedule()

                tracking_metadata = get_tracking_data.expand(date = schedule_dates_to_do)

nba_tracking_pipeline()
                