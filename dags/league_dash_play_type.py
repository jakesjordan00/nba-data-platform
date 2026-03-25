from airflow.sdk import dag, task, TaskGroup
from datetime import datetime, timedelta


@dag(
    dag_id = 'league_dash_play_type_pipeline',
    dag_display_name = 'NBA API - Synergy Play Type Pipeline',
    start_date = datetime(2025, 3, 1),
    schedule = '2/10 12-23,0-4/1 * * *',
    catchup = False,
    max_active_runs = 1,
    max_active_tasks= 4,
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(seconds=30)
    },
    tags = [
        'src - nba api',
        'synergy',
        'statistics',
        'daily',
        'schema - plays'    
    ],
    doc_md = """
"""

)
def nba_play_type_pipeline():
    from pipelines import LeagueDashAPI, ScheduleForAPI
    pipeline_nba_api = LeagueDashAPI()
    data_schedule_for_api = ScheduleForAPI()
    PlayType_options = ['Isolation','Transition','PRBallHandler','PRRollman','Postup','Spotup','Handoff','Cut','OffScreen','OffRebound','Misc']

    for pt in ['Player', 'Team']:
        for play_type in PlayType_options:
            for type_group in ['Offensive', 'Defensive']:

                @task(
                    task_id=f'synergy_{pt.lower()}_{type_group.lower()}_{play_type.lower()}',
                    task_display_name=f'Leaguedash API - Synergy {pt} {type_group} {play_type}'
            )
                def get_play_type_data(pt = pt, play_type = play_type, type_group = type_group):
                    pipeline_nba_api._re_init(
                        schema = 'plays',
                        params = {
                            'PlayType': play_type,
                            'PlayerOrTeam': pt[0], #P or T
                            'TypeGrouping': type_group
                        },
                        endpoint_friendly_name = 'pt_play_type',
                        table_base_name = 'Plays',
                        player_team = pt,
                        log_tag = f'.{play_type}'.lower(),
                        extract_tag = f'Extracting {type_group} {pt} {play_type} data via the NBA/Synergy API'
                    )
                    play_type_pipeline = pipeline_nba_api.run(date_data={})
                    play_type_data = play_type_pipeline['xcom']
                    return play_type_data

                play_type_data = get_play_type_data()
                
nba_play_type_pipeline()