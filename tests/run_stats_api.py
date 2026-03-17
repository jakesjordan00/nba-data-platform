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
for schema in [
    'adv', 
    'misc', 
    'usage',
    # 'def',
    'violations',
    ]:
    schedule_pipeline = ScheduleForAPI(schema=schema)
    completed_schedule_pipeline = schedule_pipeline.run()
    schedule_data = completed_schedule_pipeline['loaded']

    for date in schedule_data:
        from pipelines import AdvancedStatsPipeline
        adv_stats_pipeline = AdvancedStatsPipeline(date_data=date, schema=schema)
        completed_adv_stats_pipeline = adv_stats_pipeline.run()
        stats_data = completed_adv_stats_pipeline['loaded']
    # adv_stats_pipeline = AdvancedStatsPipeline(data=date['games'], schema=schema)


#Schemas: adv, misc, 