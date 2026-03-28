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
        Returns a list of formatted Game dictionaries

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




    def schedule(self, data_extract: dict) -> list:
        '''`schedule`(self, data_extract: dict)
        Returns a list of formatted Game dictionaries

        :param data: Output of fetch(). Contains game information for all games (Excluding All-Star) that are
        Completed or In-Progress as of the time of fetch
        :type data: dict
        :return schedule: List of games that are **In progress** or **Completed** this season
        :rtype: list        
        '''
        data = data_extract['leagueSchedule']
        SeasonID = data['seasonYear'][:4]
        full_schedule_games = [game['games'] for game in data['gameDates']]
        schedule_games = [game['games'] for game in data['gameDates'] if datetime.strptime(game['gameDate'], '%m/%d/%Y %H:%M:%S') <= datetime.today()]
        today = datetime.today().strftime('%d/%m/%Y')
        self.logger.info(f'Excluding games scheduled after {today}...{len(full_schedule_games)} dates -> {len(schedule_games)} dates')

        schedule_unformatted = []
        game_counts = {
            1: 0,
            2: 0,
            3: 0,
            4: 0,
            5: 0,
            6: 0
        }
        i = 0
        right_now = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        for date in schedule_games:
            for game in date:
                if datetime.strptime(game['gameDateTimeEst'], '%Y-%m-%dT%H:%M:%SZ') >= datetime.now():
                    continue
                i+=1
                gameType = int(game['gameId'][2])
                game_counts[gameType] += 1
                if gameType not in[1, 3]:
                    schedule_unformatted.append(game)
                bp = 'here'
        self.logger.info(f'Excluded games scheduled after {right_now}... {i} games total')
        schedule = [
            {
                'SeasonID': SeasonID,
                'GameID': int(g['gameId']),
                'GameIDStr': g['gameId'],             
                'GameCode': g['gameCode'],
                'GameStatus': g['gameStatus'],
                'GameStatusText': g['gameStatusText'],
                'Period': 4 if g['gameStatusText'] == 'Final' 
                            else 5 if g['gameStatusText'] == 'Final/OT'
                            else 4 + int(g['gameStatusText'][-1]) if 'Final/OT' in g['gameStatusText']
                            else int(g['gameStatusText'][1]) if g['gameStatus'] == 2 and g['gameStatusText'][0] == 'Q'
                            else 0,
                # 'GameClock': g['gameClock'],
                'GameTimeUTC': g['gameDateTimeUTC'],
                'GameTimeEST': g['gameDateTimeEst'],
                # 'GameEt': g['gameEt'],
                # 'RegulationPeriods': g['regulationPeriods'],
                'IfNecessary': g['ifNecessary'],
                'SeriesGameNumber': g['seriesGameNumber'],
                'GameLabel': g['gameLabel'],
                'GameSubLabel': g['gameSubLabel'],
                'SeriesText': g['seriesText'],                
                # 'SeriesConference': g['seriesConference'],
                # 'RoundDesc': g['poRoundDesc'],

                'GameSubtype': g['gameSubtype'],
                'IsNeutral': g['isNeutral'],
                'HomeTeam': g['homeTeam'],
                'AwayTeam': g['awayTeam'],
                }             
            for g in schedule_unformatted]



        self.logger.info(f'Preseason Games: {game_counts[1]} (Excluded)')
        self.logger.info(f'Regular Season Games: {game_counts[2]}')
        self.logger.info(f'Postseason Games: {game_counts[4]}')
        self.logger.info(f'Play-In Games: {game_counts[5]}')
        self.logger.info(f'NBA Cup Games: {game_counts[6]}')
        self.logger.info(f'All-Star Games: {game_counts[3]} (Excluded)')
        self.logger.info(f'{len(schedule)} Games queued for insert')
        return schedule

    def schedule_backfill(self, data_extract_formatted: list, db_schedule: pl.DataFrame) -> list | list[dict]:
        '''schedule_backfill
    ===
        <hr>
        
    Returns a list of dicts containing Game info for any games found to be out of date in the database

    Parameters
    -------------
    <hr>

        __data_extract_formatted__ (*list*): List of dicts containing Game info.
        __db_schedule__ (*pl.DataFrame*): Polars DataFrame containing the results of the *sql/queries/source/**schedule_backfill.sql*** query's execution

    Returns
    -------------
    <hr>

        __data_transformed__ (*list*): List of dicts containing Game info, but only for those games not up to date in the database
        
            - Will be used as the source of games to extract data for when executing downstream pipelines
        '''
        games = db_schedule['GameID'].to_list()
        self.logger.info('Now filtering out games that are up to date...')
        data_transformed = [game for game in data_extract_formatted if game['GameID'] in games]
        self.logger.info(f'{len(data_transformed)} games to backfill')
        return data_transformed


    def schedule_db(self, data_extract: dict) -> list | list[dict]:
        '''schedule_db
    ===
        <hr>
        
    Formats *data_extract* into list of dicts containing data formatted for upsert to the Schedule table in database

    Parameters
    -------------
    <hr>

        __data_extract__ (dict): Data extracted from the [scheduleLeagueV2_1 data feed](https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json)



    Returns
    -------------
    <hr>

        __db_schedule__ (list): list of dicts containing Game data. Formatted to fill the Schedule table in databse
        '''
        data = data_extract['leagueSchedule']
        SeasonID = data['seasonYear'][:4]
        schedule_games = [game for date in data['gameDates'] for game in date['games']] #For each date, then for each game on that date -> add to list
        db_schedule = []
        for game in schedule_games:
            game_type = int(game['gameId'][2])
            gt_est = datetime.strptime(game['gameDateTimeEst'], '%Y-%m-%dT%H:%M:%SZ')
            gt_utc = datetime.strptime(game['gameDateTimeUTC'], '%Y-%m-%dT%H:%M:%SZ')
            gt_htt = datetime.strptime(game['homeTeamTime'], '%Y-%m-%dT%H:%M:%SZ')
            gt_att = datetime.strptime(game['awayTeamTime'], '%Y-%m-%dT%H:%M:%SZ')
            schedule ={
                'SeasonID': SeasonID,
                'GameID': int(game['gameId']),
                'GameType': 'PRE' if game_type == 1 else 'RS' if game_type == 2 else 'PS' if game_type == 4 else 'PI' if game_type == 5 else 'AS' if game_type == 3 else 'CUP',
                'Status': game['gameStatusText'],
                'Sequence': game['gameSequence'],
                'GameTimeEST':  gt_est if gt_est > datetime(year=1900, month = 1, day = 1) else None,
                'GameTimeUTC':  gt_utc if gt_utc > datetime(year=1900, month = 1, day = 1) else None,
                'GameTimeHome': gt_htt if gt_htt > datetime(year=1900, month = 1, day = 1) else None,
                'GameTimeAway': gt_att if gt_att > datetime(year=1900, month = 1, day = 1) else None,
                'Day': game['day'],
                'Week': game['weekNumber'],
                'Label': game['gameLabel'],
                'LabelDetail': game['gameSubLabel'],
                'IsNeutral': 1 if game['isNeutral'] else 0,
                'HomeID': game['homeTeam']['teamId'],
                'HomeWins': game['homeTeam']['wins'],
                'HomeLosses': game['homeTeam']['losses'],
                'HomeScore': game['homeTeam']['score'],
                'HomeSeed': game['homeTeam']['seed'],
                'AwayID': game['awayTeam']['teamId'],
                'AwayWins': game['awayTeam']['wins'],
                'AwayLosses': game['awayTeam']['losses'],
                'AwayScore': game['awayTeam']['score'],
                'AwaySeed': game['awayTeam']['seed'],
                'IfNecessary': 1 if game['ifNecessary'] else 0,
                'SeriesText': game['seriesText'],
            }
            db_schedule.append(schedule)
            
        return(db_schedule)