import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import Pipeline, ScoreboardPipeline, BoxscorePipeline, PlayByPlayPipeline, SchedulePipeline, DailyBackfillSchedulePipeline
from connectors import SQLConnector
import polars as pl

from pipelines import DailyBackfillSchedulePipeline
schedule_pipeline = DailyBackfillSchedulePipeline()
completed_schedule_pipeline = schedule_pipeline.run()
schedule_data = completed_schedule_pipeline['transformed']['data_transformed']
bp = 'here'