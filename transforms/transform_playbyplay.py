
from __future__ import annotations
from multiprocessing import process
from transforms import transform_stints
from transforms.stint_processor import StintProcessor
import pandas as pd
import polars as pl
from datetime import datetime
import logging
import types
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.playbyplay import PlayByPlayPipeline


class Transform:

    def __init__(self, pipeline: PlayByPlayPipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass

    def playbyplay(self, playbyplay_data):
        playbyplay_data = playbyplay_data['game']['actions']
        db_actions = self.pipeline.db_actions
        db_last_action_number = self.pipeline.db_last_action_number
        try:
            matched_last_action = next({'index': i, 'action': action} for i, action in enumerate(playbyplay_data) if action['actionNumber'] == db_last_action_number)
        except StopIteration as e:
            matched_last_action = {'index': -1} 
        matched_last_index = matched_last_action['index']
        
        if self.pipeline.stint_status == 'failure':
            self.logger.warning(f"stint_status is failure. Deleting Stint and StintPlayer rows for game.")
            query = f'''delete from StintPlayer where SeasonID = {self.pipeline.boxscore_data['SeasonID']} and GameID = {self.pipeline.GameID}
delete from Stint where SeasonID = {self.pipeline.boxscore_data['SeasonID']} and GameID = {self.pipeline.GameID}'''
            self.pipeline.destination.delete_rows(query)
            bp = 'here'




        if matched_last_index != db_actions - 1:
            #delete data real quick
            self.logger.warning(f"Index of the action matching db's last action number does not match! Deleting PlayByPlay data...")
            query = f'delete from PlayByPlay where SeasonID = {self.pipeline.boxscore_data['SeasonID']} and GameID = {self.pipeline.GameID}'
            self.pipeline.destination.delete_rows(query)
            self.pipeline.db_actions = 0
            self.pipeline.db_last_action_number = 0
            bp = 'here'

        playbyplay_data, sub_groups = transform_stints.determine_substitutions(playbyplay_data, self.pipeline.boxscore_data)
        transformed_playbyplay = TransformPlayByPlay(playbyplay_data, self.pipeline.boxscore_data, db_actions, db_last_action_number)
        
        stint_processor = StintProcessor(playbyplay_data, self.pipeline.boxscore_data, sub_groups, self.pipeline.home_stats, self.pipeline.away_stats, self.pipeline.stint_status, self.pipeline.db_actions, self.pipeline.db_last_action_number)
        stints = stint_processor.process()
        data_transformed = {
            'PlayByPlay': transformed_playbyplay,
            'Stints': stints,
        }
        return data_transformed




    

def TransformPlayByPlay(playbyplay_data: list, boxscore_data: dict, db_actions: int, db_last_action_number: int) -> list:
    '''TransformPlayByPlay
==
Transforms extracted PlayByPlay data and transformed Boxscore data into a list of pbp action dicts for SQL<br>


:param list playbyplay_data: Extracted PlayByPlay data
:param dict boxscore_data: Transformed Boxscore data for Game
:param int db_actions: Count of total actions or rows a game has in the PlayByPlay table in the db    
:param int db_last_action_number: The max ActionNumber from the PlayByPlay table in the db for a game
        * We'll use this to determine what index we start at: Index matching the actionNumber of db_last_action_number

:return transformed_playbyplay: List of Transformed PlayByPlay actions to be inserted to SQL
:rtype: list
    '''
    transformed_playbyplay = []
    gameTime = 48 if boxscore_data['sql_tables']['GameExt']['Periods'] <= 4 else (5 * (boxscore_data['sql_tables']['GameExt']['Periods'] - 4))

    matched_last_action = next(({'index': i, 'action': action} for i, action in enumerate(playbyplay_data) if action['actionNumber'] == db_last_action_number), {})
    matched_last_index = matched_last_action['index'] if matched_last_action.get('index') else 0

    if matched_last_index > 0:
        start_index = matched_last_index + 1
    else:
        start_index = 0
    
    test2 = playbyplay_data[db_actions:]
    bp = 'here'
    for i, action in enumerate(playbyplay_data[start_index:]):
        SeasonID = boxscore_data['SeasonID']
        GameID = boxscore_data['GameID']
        ActionID = int(i + 1) if db_actions == 0 else int(i + start_index+ 1 )
        ActionNumber = action['actionNumber']
        Qtr = action['period']
        Clock = action['clock'].replace('PT', '').replace('M', ':').replace('S', '')
        # PointInGame = CalculatePointInGame(Clock, Qtr)
        PointInGame = action['PointInGame']
        time_formatted = f'{action['timeActual'][:-1].replace('T', ' ')}'
        TimeActual = datetime.strptime(time_formatted, '%Y-%m-%d %H:%M:%S.%f')
        ScoreHome = int(action['scoreHome'])
        ScoreAway = int(action['scoreAway'])
        Possession = action.get('possession') if action.get('possession') != 0 else None
        TeamID = action.get('teamId')
        Tricode = action.get('teamTricode')
        PlayerID = action.get('personId') if action.get('personId') != 0 else None 
        Description = action.get('description').replace("'", "''")
        SubType = action['subType'] if action['subType'] != '' else None
        IsFieldGoal = action.get('isFieldGoal') if action.get('isFieldGoal') == 1 else None
        ShotResult = action.get('shotResult')
        ShotValue = int(action['actionType'][0]) if ShotResult != None and action['actionType'] != 'freethrow' else 1 if action['actionType'] == 'freethrow' else None
        ActionType = action['actionType']
        ShotDistance = action.get('shotDistance')
        XLegacy = action.get('xLegacy')
        YLegacy = action.get('yLegacy')
        X = action.get('x')        
        Y = action.get('y')
        Area = action.get('area')
        AreaDetail = action.get('areaDetail')
        Side = action['side']
        if ShotResult == 'Made':
            ShotType = f"FG{ShotValue}M"
            PtsGenerated = ShotValue
        elif ShotResult == 'Missed':
            ShotType = f"FG{ShotValue}A"
            PtsGenerated = 0
        else:            
            ShotType = None
            PtsGenerated = None
        if action['actionType'] == 'freethrow':
            ShotType = f'FT{ShotType[3]}' #type: ignore
        Descriptor = action.get('descriptor')
        ShotActionNbr = action.get('shotActionNbr')
        Qual1 = action['qualifiers'][0] if 'qualifiers' in action.keys() and len(action['qualifiers']) > 0 else None
        Qual2 = action['qualifiers'][1] if 'qualifiers' in action.keys() and len(action['qualifiers']) > 1 else None
        Qual3 = action['qualifiers'][2] if 'qualifiers' in action.keys() and len(action['qualifiers']) > 2 else None
        PlayerIDAst = action.get('assistPersonId')
        PlayerIDBlk = action.get('blockPersonId')
        PlayerIDStl = action.get('stealPersonId')
        PlayerIDFoulDrawn = action.get('foulDrawnPersonId')
        PlayerIDJumpW = action.get('jumpBallWonPersonId')
        PlayerIDJumpL = action.get('jumpBallLostPersonId')
        OfficialID = action.get('officialId')
        QtrType = action['periodType']



        transformed_action = {
                'SeasonID': SeasonID,
                'GameID': GameID,
                'ActionID': ActionID,
                'ActionNumber': ActionNumber,
                'PointInGame': PointInGame,
                'Qtr': Qtr,
                'Clock': Clock,
                'TimeActual': TimeActual,
                'ScoreHome': ScoreHome,
                'ScoreAway': ScoreAway,
                'Possession': Possession,
                'TeamID': TeamID,
                'Tricode': Tricode,
                'PlayerID': PlayerID,
                'Description': Description,
                'SubType': SubType,
                'IsFieldGoal': IsFieldGoal,
                'ShotResult': ShotResult,
                'ShotValue': ShotValue,
                'ActionType': ActionType,
                'ShotDistance': ShotDistance,
                'Xlegacy': XLegacy,
                'Ylegacy': YLegacy,
                'X': X,
                'Y': Y,
                'Location': None,
                'Area': Area,
                'AreaDetail': AreaDetail,
                'Side': Side,
                'ShotType': ShotType,
                'PtsGenerated': PtsGenerated,
                'Descriptor': Descriptor,
                'Qual1': Qual1,
                'Qual2': Qual2,
                'Qual3': Qual3,
                'ShotActionNbr': ShotActionNbr,
                'PlayerIDAst': PlayerIDAst,
                'PlayerIDBlk': PlayerIDBlk,
                'PlayerIDStl': PlayerIDStl,
                'PlayerIDFoulDrawn': PlayerIDFoulDrawn,
                'PlayerIDJumpW': PlayerIDJumpW,
                'PlayerIDJumpL': PlayerIDJumpL,
                'OfficialID': OfficialID,
                'QtrType': QtrType
        }
        # test = transform_stints.Stint(action, transformed_action, transformed_playbyplay, HomeID, AwayID, home, away)

        transformed_playbyplay.append(transformed_action)
    return transformed_playbyplay




def CalculatePointInGame(Clock: str, Period: int):
    cMinutes = int(Clock[0:2])
    cSeconds = float(Clock[-5:])
    PointInGame = 12 - (cMinutes + (cSeconds / 60)) + ((Period - 1) * 12)
    return PointInGame

