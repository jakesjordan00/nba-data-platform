import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import Pipeline, ScoreboardPipeline, BoxscorePipeline, PlayByPlayPipeline, SchedulePipeline, DailyBackfillSchedulePipeline
from connectors import SQLConnector
import polars as pl


# schedule_pipeline = SchedulePipeline()
# completed_schedule_pipeline = schedule_pipeline.run()
# schedule_data = completed_schedule_pipeline['loaded']




# for schedule in schedule_data:
#     GameID = schedule['GameID']
#     print(f'\n                                                 {GameID}\n                                   -------------------------------------')
    
#     boxscore_pipeline = BoxscorePipeline(f'boxscore.{GameID}', schedule, 'Production')
#     completed_boxscore_pipeline = boxscore_pipeline.run()
#     boxscore_data = completed_boxscore_pipeline['loaded']
    
#     start_action_info = boxscore_pipeline.destination.cursor_query('PlayByPlay', boxscore_data['start_action_keys'])
#     db_actions = start_action_info['actions']
#     db_last_action_number = start_action_info['last_action_number']
#     stint_status = start_action_info['stint_status']

#     home_stats, away_stats = (None, None) if db_actions == 0 or stint_status == 'failure' else boxscore_pipeline.destination.stint_cursor(boxscore_data['lineup_keys'])
#     playbyplay_pipeline = PlayByPlayPipeline(f'playbyplay.{GameID}',boxscore_data, db_actions, db_last_action_number, home_stats, away_stats, stint_status, 'Production')
#     completed_playbyplay_pipeline = playbyplay_pipeline.run()
#     playbyplay_data = completed_playbyplay_pipeline['loaded']
#     bp = 'here'


# bp = 'here'



schedule_pipeline = DailyBackfillSchedulePipeline()
completed_schedule_pipeline = schedule_pipeline.run()
schedule_data = completed_schedule_pipeline['loaded']




for schedule in schedule_data:
    GameID = schedule['GameID']
    print(f'\n                                                 {GameID}\n                                   -------------------------------------')
    
    boxscore_pipeline = BoxscorePipeline(f'boxscore.{GameID}', schedule, 'Production')
    completed_boxscore_pipeline = boxscore_pipeline.run()
    boxscore_data = completed_boxscore_pipeline['loaded']
    
    start_action_info = boxscore_pipeline.destination.cursor_query('PlayByPlay', boxscore_data['start_action_keys'])
    db_actions = start_action_info['actions']
    db_last_action_number = start_action_info['last_action_number']
    stint_status = start_action_info['stint_status']

    home_stats, away_stats = (None, None) if db_actions == 0 or stint_status == 'failure' else boxscore_pipeline.destination.stint_cursor(boxscore_data['lineup_keys'])
    playbyplay_pipeline = PlayByPlayPipeline(f'playbyplay.{GameID}',boxscore_data, db_actions, db_last_action_number, home_stats, away_stats, stint_status, 'Production')
    completed_playbyplay_pipeline = playbyplay_pipeline.run()
    playbyplay_data = completed_playbyplay_pipeline['loaded']
    bp = 'here'


bp = 'here'
