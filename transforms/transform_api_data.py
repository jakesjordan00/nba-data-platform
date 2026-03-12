import logging


class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        self.data = pipeline.data if pipeline.data else None
        pass



    def start_transform(self, data_extract):
        self.data_extract = data_extract
        if data_extract['parameters']['MeasureType'] == 'Advanced':
            data_transformed = self.measure_advanced()
        return data_transformed


    def measure_advanced(self):
        data_extract = self.data_extract
        games_on_date = self.data if self.data else []
        if len(data_extract['resultSets']) > 1:
            self.logger.warning(f'Multiple result sets returned! Only configured to handle one!')
        results = data_extract['resultSets'][0]
        for i, column in enumerate(results['headers']):
            print(f"                '{column}': player[{i}],")
        result_dicts = []
        for player in results['rowSet']:
            # for game in games_on_date:
            #     if player[3] in [game['HomeID'], game['AwayID']]:

            matching_game = next((
                game for game in games_on_date 
                    if player[3] in[game['HomeID'], game['AwayID']] 
                    or player[0] in game['home_players']
                    or player[0] in game['away_players']), {})
            SeasonID = matching_game.get('SeasonID')
            GameID = matching_game.get('GameID')
            if player[3] == matching_game.get('AwayID') or player[0] in matching_game['away_players']:
                MatchupID = matching_game['HomeID']
            elif player[3] == matching_game.get('HomeID') or player[0] in matching_game['home_players']:
                MatchupID = matching_game['AwayID']
            else:
                MatchupID = 0
            player = {
                'SeasonID': SeasonID,
                'GameID': GameID,
                'TeamID': player[3],
                'MatchupID': MatchupID,
                'PlayerID': player[0],
                'Minutes': player[10],
                'OffRTG': player[12],
                'DefRTG': player[15],
                'NetRTG': player[18],
                '[Ast%]': player[20],
                'ATR': player[21],
                'AstRatio': player[22],
                '[OReb%]': player[23],
                '[DReb%]': player[24],
                '[Reb%]': player[25],
                '[TeamTO%]': player[26],
                '[EFG%]': player[28],
                '[TS%]': player[29],
                '[Usage%]': player[30],
                'Pace': player[33],
                'PacePer40': player[34],
                'PIE': player[36],
                'POSS': player[37],
                'FGM': player[38],
                'FGA': player[39],
                '[FG%]': player[42],
            }
            result_dicts.append(player)
        return(result_dicts)