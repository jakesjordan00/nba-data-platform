from airflow.sdk import dag, task, BaseHook
from datetime import datetime, timedelta
from dataclasses import asdict

@dag(
    dag_id = 'nba_schedule_pipeline',
    dag_display_name= 'NBA Schedule Games Pipeline',
    start_date = datetime(year=2026, month = 1, day = 1),
    schedule = '0 16 1 * *', #Fire at noon EDT on the first of each month (UTC-4)
    catchup = False,
    max_active_runs = 1,
    default_args= {
        'retries': 1,
        'retry_delay': timedelta(seconds = 30)
    },
    tags = [
        'src - static data feed',
        'monthly'
    ],
    doc_md = """# NBA Schedule Games Pipeline

## Overview

This DAG uses the NBA Static data feed Schedule data as its source and runs the core BoxScore and PlayByPlay pipelines for the games returned
"""
)

def nba_pipeline():
    @task
    def run_schedule():
        from pipelines import SchedulePipeline
        schedule_pipeline = SchedulePipeline()
        completed_schedule_pipeline = schedule_pipeline.run()
        schedule_data = completed_schedule_pipeline['transformed']['data_transformed']
        return schedule_data
    

    @task
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
        boxscore_data = {
            'boxscore_data': boxscore_data,
            'db_actions': db_actions,
            'db_last_action_number': db_last_action_number,
            'home_stats': home_stats,
            'away_stats': away_stats,
            'stint_status': stint_status
        }
        return boxscore_data

    @task
    def run_playbyplay(boxscore_result):
        from pipelines import PlayByPlayPipeline
        box_data = boxscore_result['boxscore_data']
        GameID = box_data['GameID']
        playbyplay_pipeline = PlayByPlayPipeline(
            pipeline_name = f'playbyplay.{GameID}',
            boxscore_data = box_data,
            db_actions = boxscore_result['db_actions'],
            db_last_action_number = boxscore_result['db_last_action_number'],
            home_stats = boxscore_result['home_stats'],
            away_stats = boxscore_result['away_stats'],
            stint_status = boxscore_result['stint_status'],
            environment = 'Production'
        )
        completed_playbyplay_pipeline = playbyplay_pipeline.run()
        loaded_playbyplay_data = completed_playbyplay_pipeline['loaded']

        return {
            'playbyplay_actions': len(loaded_playbyplay_data['PlayByPlay']),
            'stint_errors': [asdict(err) for err in loaded_playbyplay_data['Stints'].errors]
        }

    games = run_schedule()

    boxscore_results = run_boxscore.expand(game = games)

    run_playbyplay.expand(boxscore_result = boxscore_results)

nba_pipeline()