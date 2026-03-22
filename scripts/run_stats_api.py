import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors import APIDataConnector, SQLConnector, StaticDataConnector
from pipelines import ScheduleForAPI
from pipelines import LeagueDashAPI


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
        play_type_pipeline = LeagueDashAPI(
            schema = 'plays',
            params = {
                'PlayType': play_type,
                'PlayerOrTeam': pt[0], #P or T
            },
            endpoint_friendly_name = 'pt_play_type',
            table_base_name = 'Plays',
            player_team = pt,
            log_tag = f'.{play_type}'.lower()
        )
        completed_play_type_pipeline = play_type_pipeline.run(date_data={})
        play_type_data = completed_play_type_pipeline['loaded']

        type_group = 'Offensive' if play_type_pipeline._endpoint.params['TypeGrouping'] == 'Defensive' else 'Defensive'
        play_type_pipeline._endpoint.params = {
            **play_type_pipeline._endpoint.params,
            'TypeGrouping': type_group
        }
        completed_play_type_pipeline = play_type_pipeline.run(date_data={})
        play_type_data = completed_play_type_pipeline['loaded']


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
                adv_stats_pipeline = LeagueDashAPI(
                    schema = 'tracking',
                    params = {
                        'PlayerOrTeam': pt,
                        'PtMeasureType': tracking_measure,
                    },
                    endpoint_friendly_name = 'pt_tracking',
                    table_base_name = tracking_measure,
                    player_team = pt,
                    log_tag = f'.{pt}_{tracking_measure}'.lower()
                )
                completed_adv_stats_pipeline = adv_stats_pipeline.run(date_data=date)
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
        hustle_pipeline = LeagueDashAPI(
            schema = 'tracking',
            params = {},
            endpoint_friendly_name = f'{pt}_hustle'.lower(),
            table_base_name = 'Hustle',
            player_team = pt,
            log_tag = f'.{pt}_hustle'.lower()
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
            adv_stats_pipeline = LeagueDashAPI(
                schema=schema,
                params = {
                    'MeasureType': measure_type,
                },
                endpoint_friendly_name = f'{pt}_stats'.lower(),
                table_base_name = 'Box',
                player_team = pt,
                log_tag = f'.{pt.lower()}'
            )
            completed_adv_stats_pipeline = adv_stats_pipeline.run(date_data=date)
            stats_data = completed_adv_stats_pipeline['loaded'] 

#endregion Advanced Metrics

