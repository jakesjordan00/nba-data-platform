import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nba_data_platform.pipelines import Pipeline, ScoreboardPipeline, BoxscorePipeline, PlayByPlayPipeline, SchedulePipeline, DailyBackfillSchedulePipeline
from nba_data_platform.connectors import SQLConnector
import polars as pl

from nba_data_platform.pipelines import SchedulePipeline
schedule_pipeline = SchedulePipeline()
completed_schedule_pipeline = schedule_pipeline.run()
schedule_data = completed_schedule_pipeline['transformed']['data_transformed']
data = completed_schedule_pipeline['loaded']

test = len(schedule_data)
test2 = len(data)
bp = 'here'