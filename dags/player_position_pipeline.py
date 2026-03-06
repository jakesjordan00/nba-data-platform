from airflow.sdk import dag, task, BaseHook
from datetime import datetime, timedelta

@dag(
    dag_id = 'player_position_pipeline',
    dag_display_name = f'Player Position Pipeline',
    description = "Queries db for a count of each players' starts at each position for the current season. Updates Position value in Player table to most frequently occuring position for each player.",
    schedule = '0 17 * * 0', #Fire at 1pm EDT every sunday
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries': 0,
        'retry_delay': timedelta(minutes = 5)
    }
)


def player_position_pipeline():
    @task
    def update_player():
        from pipelines.player_positions import PlayerPositionPipeline
        player_position_pipeline = PlayerPositionPipeline()
        rows_affected = player_position_pipeline.run()
        return rows_affected
    
    update_player()


player_position_pipeline()