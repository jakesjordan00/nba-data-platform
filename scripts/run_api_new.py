import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors import APIDataConnector, SQLConnector, StaticDataConnector
from pipelines import ScheduleForAPI
from pipelines import AdvancedStatsPipeline




nba_api_pipeline = AdvancedStatsPipeline()




#region Synergy Playtype Stats
for pt in [
    'Team', 
    'Player'
]:
    for play_type in[
        'Isolation',
        'Transition',
        'PRBallHandler',
        'PRRollman',
        'Postup',
        'Spotup',
        'Handoff',
        'Cut',
        'OffScreen',
        'OffRebound',
        'Misc'
    ]:
        for type_group in [
            'Offensive', 
            'Defensive'
        ]:
            nba_api_pipeline._re_init(
                schema = 'plays',
                params = {
                    'PlayType': play_type,
                    'PlayerOrTeam': pt[0], #P or T
                    'TypeGrouping': type_group
                },
                endpoint_friendly_name = 'pt_play_type',
                tracking_table = 'Plays',
                player_team = pt,
                log_tag = f'.{play_type}'.lower(),
                extract_tag = f'Synergy API - {type_group} {pt} {play_type}'
            )
            play_type_pipeline = nba_api_pipeline.run(date_data={})
            play_type_data = play_type_pipeline['loaded']


#endregion Synergy Playtype Stats





#region SecondSpectrum Tracking

for pt in [
    'Team', 
    'Player'
]:
    for tracking_measure in[
        'Drives',
        'Defense',
        'CatchShoot',
        'Passing',
        'Possessions',
        'PullUpShot',
        'Rebounding',
        'Efficiency',
        'SpeedDistance',
        'ElbowTouch',
        'PostTouch',
        'PaintTouch'
    ]:
        schedule_pipeline = ScheduleForAPI(
            schema = 'tracking',
            tracking_measure = tracking_measure,
            player_team = pt
        )
        completed_schedule_pipeline = schedule_pipeline.run()
        schedule_data = completed_schedule_pipeline['loaded']
        for date in schedule_data:
                nba_api_pipeline._re_init(
                    schema = 'tracking',
                    params = {
                        'PlayerOrTeam': pt,
                        'PtMeasureType': tracking_measure,
                    },
                    endpoint_friendly_name = 'pt_tracking',
                    tracking_table = tracking_measure,
                    player_team = pt,
                    log_tag = f'.{pt}_{tracking_measure}'.lower(),
                    extract_tag = date
                )
                completed_adv_stats_pipeline = nba_api_pipeline.run(date_data=date)
                stats_data = completed_adv_stats_pipeline['loaded']

#endregion SecondSpectrum Tracking  



#region Hustle Stats
for pt in [
    'Team', 
    'Player'
]:
    schedule_pipeline = ScheduleForAPI(
        schema='tracking', 
        tracking_measure=f'Hustle',
        player_team = pt
    )
    completed_schedule_pipeline = schedule_pipeline.run()
    schedule_data = completed_schedule_pipeline['loaded']
    for date in schedule_data:
        hustle_pipeline = AdvancedStatsPipeline(
            schema = 'tracking',
            params = {},
            endpoint_friendly_name = f'{pt}_hustle'.lower(),
            tracking_table = 'Hustle',
            player_team = pt,
            log_tag = f'.{pt}_hustle'.lower(),
            extract_tag = ''
        )
        completed_hustle_pipeline = hustle_pipeline.run(date_data=date)
        hustle_data = completed_hustle_pipeline['loaded']

#endregion Hustle Stats



#region Advanced Metrics
for pt in [
    'Team', 
    'Player'
]:
    schema_config = {
        # 'adv': 'Advanced',
        # 'misc': 'Misc',
        # 'usage': 'Usage',   #No Usage for teams
        # 'def': 'Defense',
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
                },
                endpoint_friendly_name = f'{pt}_stats'.lower(),
                tracking_table = 'Box',
                player_team = pt,
                log_tag = f'.{pt.lower()}',
                extract_tag = ''
            )
            completed_adv_stats_pipeline = adv_stats_pipeline.run(date_data=date)
            stats_data = completed_adv_stats_pipeline['loaded'] 

#endregion Advanced Metrics

