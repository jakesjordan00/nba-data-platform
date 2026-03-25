from airflow.sdk import dag, task, TaskGroup
from datetime import datetime, timedelta
DOCUMENTATION = """# NBA SecondSpectrum Tracking Pipeline
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
            'second spectrum',
            'statistics',
            'schema - tracking',        
        ],
        **kwargs
    )
    def nba_tracking_pipeline():
        from pipelines import LeagueDashAPI, ScheduleForAPI
        pipeline_nba_api = LeagueDashAPI()
        data_schedule_for_api = ScheduleForAPI()
        PtMeasureType_options = ['Drives', 'Defense', 'CatchShoot', 'Passing', 'Possessions', 'PullUpShot', 'Rebounding', 'Efficiency', 'SpeedDistance', 'ElbowTouch', 'PostTouch', 'PaintTouch']
        
        for pt in ['Player', 'Team']:
            for tracking_measure in PtMeasureType_options:
                tg_id = f'{pt.lower()}_tracking_{tracking_measure.lower()}'
                tg_name = f'{pt} {tracking_measure} - tracking.{pt}{tracking_measure}'
                with TaskGroup(group_id=tg_id, group_display_name=tg_name) as taskgroup:
                    t_id = f'schedule_{pt.lower()}_tracking'
                    t_name = f'Schedule - {pt} {tracking_measure}'
                    @task(task_id=t_id,task_display_name=t_name)
                    def get_schedule(pt = pt, tracking_measure = tracking_measure):
                        data_schedule_for_api._re_init(
                            schema = 'tracking', 
                            table_base_name = tracking_measure,
                            player_team = pt,
                            log_tag = f'.{pt}_{tracking_measure}'.lower(),
                            where_addition = 'and s.GameTimeEST >= cast(getdate()-2 as date)'
                        )
                        completed_schedule_pipeline = data_schedule_for_api.run()
                        schedule_data = completed_schedule_pipeline['loaded']
                        return schedule_data
                    
                    
                    t_id = f'second_spectrum_tracking_{pt.lower()}_{tracking_measure}'
                    t_name = f'SecondSpectrum Tracking - {pt} {tracking_measure}'
                    @task(task_id=t_id,task_display_name=t_name)
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


build_pipeline_with_parameters(
    dag_id = 'league_dash_tracking_pipeline_hourly',
    display_name = 'NBA API - Hourly SecondSpectrum Tracking Pipeline',
    schedule = '0 16-23,0-8 * * *',
    where_addition = 'and s.GameTimeEST >= cast(getdate()-2 as date)',
    schedule_tag = 'hourly',
    start_date = datetime(2025, 3, 1),
    catchup = False,
    max_active_runs = 1,
    max_active_tasks= 4,
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(seconds=30)
    },
    doc_md = DOCUMENTATION
)


build_pipeline_with_parameters(
    dag_id = 'league_dash_tracking_pipeline_daily',
    display_name = 'NBA API - Daily SecondSpectrum Tracking Pipeline',
    schedule = '45 16 * * *', #16 = Noon EST -> 12:45pm EST
    schedule_tag = 'daily',
    start_date = datetime(2025, 3, 1),
    catchup = False,
    max_active_runs = 1,
    max_active_tasks= 4,
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(seconds=30)
    },
    doc_md = DOCUMENTATION
)

                