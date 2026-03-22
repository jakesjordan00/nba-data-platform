

from airflow.sdk import dag, task, BaseHook
from datetime import datetime, timedelta
import textwrap

@dag(
    dag_id = 'nba_advanced_stats_pipeline',
    dag_display_name = 'NBA Advanced Stats Pipeline',
    start_date = datetime(year=2026, month=3, day=10),
    schedule = '* */1 * * *',
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(seconds=30)
    },
    doc_md = textwrap.dedent(
    text="""Using Schedule data from our database, pulls all games that are in progress or completed prior to the start of the run, excluding Pre-season.\n

"""
    )
)
def nba_advanced_stats_pipeline():
    @task
    def get_schedule():
        from pipelines import ScheduleForAPI
        schedule_pipeline = ScheduleForAPI(schema='adv')
        completed_schedule_pipeline = schedule_pipeline.run()
        schedule_data = completed_schedule_pipeline['loaded']
        return schedule_data
    

    @task
    def player_advanced(date):
        from pipelines import LeagueDashAPI
        adv_stats_pipeline = LeagueDashAPI(data=date['data'], schema = 'adv')
        adv_stats_pipeline.source.player_stats.params = {**adv_stats_pipeline.source.player_stats.params, 'DateFrom': date['date'], 'DateTo': date['date']}
        completed_adv_stats_pipeline = adv_stats_pipeline.run()
        data = completed_adv_stats_pipeline['loaded']

    schedule_data = get_schedule()

    player_advanced_data = player_advanced.expand(date = schedule_data)

nba_advanced_stats_pipeline()