from airflow.sdk import dag, task, BaseHook, TaskGroup
from datetime import datetime, timedelta
from pathlib import Path
DOCUMENTATION = """# NBA Advanced Metrics Pipeline 

## Overview

This DAG pulls data from the NBA Player and Team stats API, transforms, and stores data for downstream analytics.

### Covers the following Player/Team stat categories and loads to respective table(s): 
- __Advanced__ -> adv.PlayerBox, adv.TeamBox
- __Defense__ -> def.PlayerBox, def.TeamBox
- __Violations__ -> violations.PlayerBox, violations.TeamBox
- __Usage__ _(player only)_ -> usage.PlayerBox
- __Four Factors__ _(team only)_ -> ffactors.TeamBox

---

## APIs
- **leaguedashplayerstats**
- **leaguedashteamstats**

## Walkthrough

First, we intialize *LeagueDashAPI* and *ScheduleForAPI* Classes.
- These classes are configured to be initialized the once, then re-initialized with specific parameters for what we need at time of execution

We then enter the following loop:
```python
for pt in ['Player','Team']:
        for schema, measure_type in {
            'adv': 'Advanced',
            'misc': 'Misc',
            'def': 'Defense',
            'violations': 'Violations',
            'usage': 'Usage',
            'ffactors': 'Four Factors',
        }.items():
```
We'll use the *pt* value and the *schema* value to get our table name: f'{schema}.{pt}Box'

_get_schedule_
- With that, we query Schedule and PlayerBox/TeamBox, with a left join to our table to see which dates we need to check in our run
- This will also give us data for every game on the dates we need to do so we can match Player and Team stats to the games in which they were recorded
- Hourly will only look at the past two days while daily will check all dates with data potentially missing

_get_measure_type_data_
- Knowing the dates to do, we'll loop through each date and pass the date value to the API as the *DateFrom* and *DateTo* params
- We'll also pass our _measure_type_ value

- Once data is returned, match GameIDs, finish formatting and load."""

def build_pipeline_with_parameters(dag_id, display_name, schedule, where_addition = '', schedule_tag = 'daily', **kwargs):
    @dag(
        dag_id = dag_id,
        dag_display_name = display_name,
        schedule = schedule,
        doc_md = DOCUMENTATION,
        tags = [
            schedule_tag, 'src - nba api', 'statistics',
            'schema - adv', 'schema - def', 
            'schema - violations', 'schema - usage', 
            'schema - ffactors', 'schema - misc'
        ],
        **kwargs
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
                tg_id = f'{pt.lower()}_advanced_metrics_{schema}'
                tg_name = f'{pt} {measure_type} - {schema}.{pt}Box'
                with TaskGroup(group_id=tg_id,group_display_name=tg_name) as taskgroup:
                    
                    t_id = f'schedule_{pt.lower()}_{schema}'
                    t_name = f'Schedule - {pt} {measure_type}'
                    @task(task_id=t_id,task_display_name=t_name)
                    def get_schedule(schema = schema, pt = pt, measure_type = measure_type):
                        data_schedule_for_api._re_init(
                            schema = schema,
                            table_base_name = 'Box',
                            player_team = pt,
                            log_tag = f'.{pt.lower()}',
                            where_addition = where_addition
                            )
                        schedule_pipeline = data_schedule_for_api.run()
                        schedule_data = schedule_pipeline['transformed']['data_transformed']
                        return schedule_data

                    t_id = f'league_dash_{pt.lower()}_{schema}'
                    t_name = f'Leaguedash API - {pt} {measure_type}'
                    @task(task_id=t_id,task_display_name=t_name)
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
                        measure_type_metadata = measure_type_pipeline['xcom']


                    schedule_dates_to_do = get_schedule()

                    measure_type_metadata = get_measure_type_data.expand(date = schedule_dates_to_do)


    nba_advanced_metrics_pipeline()

build_pipeline_with_parameters(
    dag_id = 'league_dash_advanced_metrics_pipeline_hourly',
    display_name = 'NBA API - Hourly Advanced Metrics Pipeline',
    schedule = '8 16-23,0-8 * * *',
    where_addition = 'and s.GameTimeEST >= cast(getdate()-2 as date)',
    schedule_tag =  'hourly',
    start_date = datetime(year=2026, month=3, day=10),
    catchup = False,
    max_active_runs = 1,
    max_active_tasks= 4,
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(seconds=30)
    }
)

build_pipeline_with_parameters(
    dag_id = 'league_dash_advanced_metrics_pipeline_daily',
    display_name = 'NBA API - Daily Advanced Metrics Pipeline',
    schedule = '45 20 * * *', #20 = 4pm EST -> 4:45pm EST
    schedule_tag =  'daily',
    start_date = datetime(year=2026, month=3, day=10),
    catchup = False,
    max_active_runs = 1,
    max_active_tasks= 4,
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(seconds=30)
    }
)