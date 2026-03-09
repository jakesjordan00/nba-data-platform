
from airflow.sdk import dag, task, BaseHook
from datetime import datetime, timedelta
import textwrap


@dag(
    dag_id = 'daily_lineups_pipeline',
    dag_display_name = 'Daily Lineups Pipeline',
    start_date = datetime(year=2026, month=3, day=1),
    schedule = '*/15 12-22 * 10-12,1-5 *',
    catchup = False,
    max_active_runs = False,
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(seconds = 30)
    },
    doc_md = textwrap.dedent("""

""")
)
def daily_lineups_pipeline():
    @task()
    def daily_lineups():
        from pipelines import DailyLineupsPipeline
        daily_lineups_pipeline = DailyLineupsPipeline('daily_lineups')
        completed_daily_lineups_pipeline = daily_lineups_pipeline.run()
        return completed_daily_lineups_pipeline['data']
        
    daily_lineups()
daily_lineups_pipeline()