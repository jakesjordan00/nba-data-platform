import logging
from datetime import datetime
class Transform:

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass


    def daily_lineups(self, data_extract: list):
        self.data_extract = data_extract
        self.logger.info(f'{len(self.data_extract)} Games')
        # self.data_filtered = self.filter_started_and_completed_games()
        # self.logger.info(f'{len(self.data_filtered)} Games yet to start or in Q1')
        data_transformed = self.transform_daily_lineups()
        bp = 'here'
        return data_transformed

    def filter_started_and_completed_games(self):
        
        return [game for game in self.data_extract if game['gameStatus'] == 1 or (game['gameStatus'] == 2 and '1st Qtr' in game['gameStatusText'])]


    def transform_daily_lineups(self):
        player_rows = []
        for game in self.data_extract:
            short_season = int(game['gameId'][3:5])
            SeasonID = f'20{short_season}' if short_season < 90 else f'19{short_season}'
            GameID = int(game['gameId'])
            self.logger.info(f'{GameID} - {game['awayTeam']['teamAbbreviation']} @ {game['homeTeam']['teamAbbreviation']}')
            teams = [game['homeTeam'], game['awayTeam']]
            for team in teams:
                TeamID = team['teamId']
                MatchupID = next((t['teamId'] for t in teams if t['teamId'] != TeamID), None)
                for player in team['players']:
                    player['seasonId'] = SeasonID
                    player['gameId'] = GameID
                    player['matchupId'] = MatchupID
                    player_rows.append(format(player))
                self.logger.info(f'{team['teamAbbreviation']}: {len(team['players'])} players')
        return player_rows





def format(player: dict):
    player_row = {
        'SeasonID': player['seasonId'],
        'GameID': player['gameId'],
        'TeamID': player['teamId'],
        'MatchupID': player['matchupId'],
        'PlayerID': player['personId'],
        'Position': player['position'] if player['position'] != '' else None,
        'LineupStatus': player['lineupStatus'],
        'RosterStatus': player['rosterStatus'],
        'Timestamp': datetime.strptime(player['timestamp'], '%Y-%m-%dT%H:%M:%S')
    }
    return player_row