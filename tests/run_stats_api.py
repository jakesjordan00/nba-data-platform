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
from pipelines import AdvancedStatsPipeline

for tracking_measure in[
    #'Drives',           #done
    #'Defense',          #done
    #'CatchShoot',
    #'Passing',          #done
    # 'Possessions',
    # 'PullUpShot',
    # 'Rebounding',
    'Efficiency',
    'SpeedDistance',
    'ElbowTouch',
    'PostTouch',
    'PaintTouch'
]:
    schedule_pipeline = ScheduleForAPI(tracking_measure=tracking_measure)
    completed_schedule_pipeline = schedule_pipeline.run()
    schedule_data = completed_schedule_pipeline['loaded']
    for date in schedule_data:
        for pt in [
            'Team', 
            'Player'
        ]:
            adv_stats_pipeline = AdvancedStatsPipeline(
                schema = 'tracking',
                params = {
                    'PlayerOrTeam': pt,
                    'PtMeasureType': tracking_measure,
                },
                tracking_table = tracking_measure,
                player_team = pt
            )
            completed_adv_stats_pipeline = adv_stats_pipeline.run(date_data=date)
            stats_data = completed_adv_stats_pipeline['loaded']

     
    

schema_config = {
    'adv': 'Advanced',
    'misc': 'Misc',
    'usage': 'Usage',
    'def': 'Defense',
    'violations': 'Violations',
    # 'scoring': 'Scoring',
}
for schema, measure_type in schema_config.items():
    schedule_pipeline = ScheduleForAPI(schema=schema)
    completed_schedule_pipeline = schedule_pipeline.run()
    schedule_data = completed_schedule_pipeline['loaded']

    for date in schedule_data:
        adv_stats_pipeline = AdvancedStatsPipeline(
            schema=schema,
            params = {
                'MeasureType': measure_type,
            }
        )
        completed_adv_stats_pipeline = adv_stats_pipeline.run(date_data=date)
        stats_data = completed_adv_stats_pipeline['loaded']
    # adv_stats_pipeline = AdvancedStatsPipeline(data=date['games'], schema=schema)


#Schemas: adv, misc, 