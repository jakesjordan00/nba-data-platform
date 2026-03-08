
from airflow.sdk import dag, task, BaseHook
from datetime import datetime, timedelta



@dag(
    dag_id = 'daily_lineups_pipeline',
    dag_display_name = 'Daily Lineups Pipeline',
    start_date = datetime(year=2026, month=3, day=1),
    schedule = '*/15 12-22 * 10-12, 1-5 *'
)