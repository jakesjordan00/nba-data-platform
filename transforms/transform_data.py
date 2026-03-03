import pandas as pd
import polars as pl
from datetime import datetime
import logging



class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass


    def scoreboard(self, data: dict) -> list:
        '''
        Returns a list of formatted Scoreboard dictionaries

        :param data: Output of fetch(). Contains game information for today's games
        :type data: dict
        :return scoreboard: List of games taking place today that are **In progress** or **Completed**
        :rtype: list
        '''
        data = data['scoreboard']['games']
        scoreboard = [
            {
                'SeasonID': int(f'20{g['gameId'][3:5]}'),
                'GameID': int(g['gameId']),
                'GameIDStr': g['gameId'],             
                'GameCode': g['gameCode'],
                'GameStatus': g['gameStatus'],
                'GameStatusText': g['gameStatusText'],
                'Period': g['period'],
                'GameClock': g['gameClock'],
                'GameTimeUTC': g['gameTimeUTC'],
                'GameEt': g['gameEt'],
                'RegulationPeriods': g['regulationPeriods'],
                'IfNecessary': g['ifNecessary'],
                'SeriesGameNumber': g['seriesGameNumber'],
                'GameLabel': g['gameLabel'],
                'GameSubLabel': g['gameSubLabel'],
                'SeriesText': g['seriesText'],
                'SeriesConference': g['seriesConference'],
                'RoundDesc': g['poRoundDesc'],
                'GameSubtype': g['gameSubtype'],
                'IsNeutral': g['isNeutral'],
                'HomeTeam': g['homeTeam'],
                'AwayTeam': g['awayTeam'],
                }             
            for g in data]
        
        #add T+10m to tipoff as a condition
        not_started = [game for game in scoreboard if game['GameStatus'] == 1] 
        log_ns = f'{len(not_started)} games yet to start' if len(not_started) != 1 else f'{len(not_started)} game yet to start'
        log_ns = f'{log_ns}: {', '.join(str(game['GameID']) for game in not_started)}'
        self.logger.info(log_ns)

        in_progress = [game for game in scoreboard if game['GameStatus'] == 2]
        log_ip = f'{len(in_progress)} games are in progress' if len(in_progress) != 1 else f'{len(in_progress)} game is in progress'
        log_ip = f'{log_ip}: {', '.join(str(game['GameID']) for game in in_progress)}'
        self.logger.info(log_ip)

        completed = [game for game in scoreboard if game['GameStatus'] == 3]
        log_c = f'{len(completed)} games have concluded' if len(completed) != 1 else f'{len(completed)} game has concluded'
        log_c = f'{log_c}: {', '.join(str(game['GameID']) for game in completed)}'
        self.logger.info(log_c)
        
        games = [game for game in scoreboard if game['GameStatus'] != 1] 
        return games

