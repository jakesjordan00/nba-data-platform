import logging

class Transform:

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass


    def daily_lineups(self, data_extract: list):
        data_transformed = self.transform_daily_lineups(data_extract)
        bp = 'here'
        return data_transformed



    def transform_daily_lineups(self, data_extract: list):
        self.logger.info(f'{len(data_extract)} Games')
        player_rows = []
        for game in data_extract:
            short_season = int(game['gameId'][3:5])
            SeasonID = f'20{short_season}' if short_season < 90 else f'19{short_season}'
            GameID = int(game['gameId'])
            self.logger.info(f'{GameID} - {game['awayTeam']['teamAbbreviation']} @ {game['homeTeam']['teamAbbreviation']}')
            for team in [game['homeTeam'], game['awayTeam']]:
                for player in team['players']:
                    player['seasonId'] = SeasonID
                    player['gameId'] = GameID
                    player_rows.append(format(player))
                self.logger.info(f'{team['teamAbbreviation']}: {len(team['players'])} players')
        # self.logger.info(f'Transformed to {len(player_rows)} rows, one for each player.')
        return player_rows


def format(player: dict):
    player_row = {
        'SeasonID': player['seasonId'],
        'GameID': player['gameId'],
        'TeamID': player['teamId'],
        'PlayerID': player['personId'],
        'Position': player['position'],
        'LineupStatus': player['lineupStatus'],
        'RosterStatus': player['rosterStatus'],
        'Timestamp': player['timestamp']
    }
    return player_row