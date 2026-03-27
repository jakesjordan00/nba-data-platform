import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors import APIDataConnector, SQLConnector, StaticDataConnector
from pipelines import ScheduleForAPI, LeagueDashAPI


pipeline_nba_api = LeagueDashAPI()
data_schedule_for_api = ScheduleForAPI()

for pt in [
    'Player',
    'Team',
]:
    #region Synergy Playtype Stats
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
            pipeline_nba_api._re_init(
                schema = 'plays',
                params = {
                    'PlayType': play_type,
                    'PlayerOrTeam': pt[0], #P or T
                    'TypeGrouping': type_group
                },
                endpoint_friendly_name = 'pt_play_type',
                table_base_name = 'Plays',
                player_team = pt,
                log_tag = f'.{play_type}'.lower(),
                extract_tag = f'Extracting {type_group} {pt} {play_type} data via the NBA/Synergy API'
            )
            play_type_pipeline = pipeline_nba_api.run(date_data={})
            play_type_data = play_type_pipeline['loaded']    
    #endregion Synergy Playtype Stats

    
    #region SecondSpectrum Tracking
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
        data_schedule_for_api._re_init(
            schema = 'tracking', 
            table_base_name = tracking_measure,
            player_team = pt,
            log_tag = f'.{pt}_{tracking_measure}'.lower(),
        )
        completed_schedule_pipeline = data_schedule_for_api.run()
        schedule_data = completed_schedule_pipeline['transformed']['data_transformed']
        for date in schedule_data:
            pipeline_nba_api._re_init(
                schema = 'tracking',
                params = {
                    'PlayerOrTeam': pt,
                    'PtMeasureType': tracking_measure,
                },
                endpoint_friendly_name = 'pt_tracking',
                table_base_name = tracking_measure,
                player_team = pt,
                log_tag = f'.{pt}_{tracking_measure}'.lower(),
                extract_tag = f'Extracting {pt} {tracking_measure} data from {date['date']} via the NBA/SecondSpectrum API'
            )
            completed_tracking_pipeline = pipeline_nba_api.run(date_data=date)
            tracking_metadata = completed_tracking_pipeline['xcom']
    #endregion SecondSpectrum Tracking

    
    #region Advanced Metrics
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
        data_schedule_for_api._re_init(
            schema = schema,
            table_base_name = 'Box',
            player_team = pt,
            log_tag = f'.{pt.lower()}',
            )
        completed_schedule_pipeline = data_schedule_for_api.run()
        schedule_data = completed_schedule_pipeline['transformed']['data_transformed']
        for date in schedule_data:
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
            advanced_box_pipeline = pipeline_nba_api.run(date_data=date)
            advanced_box_data = advanced_box_pipeline['loaded'] 

    #endregion Advanced Stats


    #region Hustle Stats
    data_schedule_for_api._re_init(
        schema='tracking', 
        table_base_name='Hustle',
        player_team = pt,
        log_tag = f'.{pt}_hustle'.lower()
    )
    completed_schedule_pipeline = data_schedule_for_api.run()
    schedule_data = completed_schedule_pipeline['transformed']['data_transformed']
    for date in schedule_data:
        pipeline_nba_api._re_init(
        schema = 'tracking',
        params = {},
        endpoint_friendly_name = f'{pt}_hustle'.lower(),
        table_base_name = 'Hustle',
        player_team = pt,
        log_tag = f'.{pt}_hustle'.lower(),
        extract_tag = f'Extracting {pt} Hustle data from {date['date']} via the NBA API'
        )
        hustle_pipeline = pipeline_nba_api.run(date_data=date)
        hustle_data = hustle_pipeline['loaded']


    #endregion Hustle Stats

