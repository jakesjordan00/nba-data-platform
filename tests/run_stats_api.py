import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors import APIDataConnector, SQLConnector, StaticDataConnector

# #TODO
# from pipelines import DailyBackfillSchedulePipeline
# backfill_pipe = DailyBackfillSchedulePipeline()
# go = backfill_pipe.run()
# bp = 'here'



from pipelines import ScheduleForAPI
schedule_pipeline = ScheduleForAPI()
completed_schedule_pipeline = schedule_pipeline.run()
schedule_data = completed_schedule_pipeline['loaded']

from pipelines import AdvancedStatsPipeline
for date in schedule_data:
    adv_stats_pipeline = AdvancedStatsPipeline(date['games'])
    adv_stats_pipeline.source.player_stats.params = {
        **adv_stats_pipeline.source.player_stats.params,
        'DateFrom': date['date'],
        'DateTo': date['date'],
        
        }
    completed_adv_stats_pipeline = adv_stats_pipeline.run()
    stats_data = completed_adv_stats_pipeline['loaded']



