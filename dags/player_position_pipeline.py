from airflow.sdk import dag, task, BaseHook
from datetime import datetime, timedelta

@dag(
    dag_id = 'player_position_pipeline',
    dag_display_name = f'Player Position Pipeline',
    start_date = datetime(year=2026, month=3, day=1),
    schedule = '0 17 * * 0', #Fire at 1pm EDT every sunday
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries': 0,
        'retry_delay': timedelta(minutes = 5)
    },
    tags = [
        'source - sql'
        'weekly',
        'metadata',
        'positions'
    ],
    description = "Queries db for a count of each players' starts at each position for the current season. Updates Position value in Player table to most frequently occuring position for each player.",

)


def player_position_pipeline():
    def update_player():
        from pipelines.player_positions import PlayerPositionPipeline
        player_position_pipeline = PlayerPositionPipeline()
        rows_affected = player_position_pipeline.run()
        return rows_affected
    
    update_player()


player_position_pipeline()