import logging


class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        self.data = pipeline.data if pipeline.data else None
        pass



    def start_transform(self, data_extract):
        self.data_extract = data_extract
        self.games_on_date = self.data if self.data else []
        if len(self.data_extract['resultSets']) > 1:
            self.logger.warning(f'Multiple result sets returned! Only configured to handle one!')
        self.results = self.data_extract['resultSets'][0]
        if self.pipeline.schema == 'adv':
            data_transformed = self.measure_advanced()
        elif self.pipeline.schema == 'misc':
            data_transformed = self.measure_misc()
        elif self.pipeline.schema == 'usage':
            data_transformed = self.measure_usage()
        elif self.pipeline.schema == 'def':
            data_transformed = self.measure_defensive()
        elif self.pipeline.schema == 'violations':
            data_transformed = self.measure_violations()
        return data_transformed

    def measure_defensive(self):
        '''measure_defensive(self)
    ===
    When MeasureType == 'Defensive', format results
        '''
        for i, column in enumerate(self.results['headers']):
            print(f"                '{column}': player[{i}],")
        result_dicts = []
        for player in self.results['rowSet']:
            self._match_game(player=player)
            player = {
                'SeasonID':     self.SeasonID,
                'GameID':       self.GameID,
                'TeamID':       self.TeamID,
                'MatchupID':    self.MatchupID,
                'PlayerID':     player[0],
            }
            result_dicts.append(player)
            
            print(f'create table {self.pipeline.schema}.PlayerBox(')
            print('SeasonID          int,')
            print('GameID            int,')
            print('TeamID            int,')
            print('MatchupID         int,')
            print('PlayerID          int,')
            for column in player.keys():
                print(f'{column}          decimal(18,3),')
        return result_dicts
    
    def measure_violations(self):
        '''measure_violations(self)
    ===
    When MeasureType == 'Violations', format results
        '''
        result_dicts = []
        for player in self.results['rowSet']:
            self._match_game(player=player)
            player = {
                'SeasonID':             self.SeasonID,
                'GameID':               self.GameID,
                'TeamID':               self.TeamID,
                'MatchupID':            self.MatchupID,
                'PlayerID':             player[0],
                'Travel':               player[10],
                'DblDribble':           player[11],
                'Inbound':              player[14],
                'Backcourt':            player[15],
                'Palming':              player[17],
                'OffFoul':              player[18],
                'Off3':                 player[13],
                'OffGoaltend':          player[16],
                'Def3':                 player[19],
                'DefGoaltend':          player[21],
                'Charge':               player[20],
                'Lane':                 player[22],
                'JumpBall':             player[23],
                'KickedBall':           player[24],
                'DiscDribble':          player[12],
            }
            result_dicts.append(player)
        return result_dicts


    def measure_advanced(self):
        '''measure_advanced(self)
    ===
    When MeasureType == 'Advanced', format results
        '''
        # for i, column in enumerate(self.results['headers']):
        #     print(f"                '{column}': player[{i}],")
        result_dicts = []
        for player in self.results['rowSet']:
            self._match_game(player=player)
            player = {
                'SeasonID':     self.SeasonID,
                'GameID':       self.GameID,
                'TeamID':       self.TeamID,
                'MatchupID':    self.MatchupID,
                'PlayerID':     player[0],
                'Minutes':      player[10],
                'OffRTG':       player[12],
                'DefRTG':       player[15],
                'NetRTG':       player[18],
                'Ast%':         player[20],
                'ATR':          player[21],
                'AstRatio':     player[22],
                'OReb%':        player[23],
                'DReb%':        player[24],
                'Reb%':         player[25],
                'TeamTO%':      player[26],
                'EFG%':         player[28],
                'TS%':          player[29],
                'Usage%':       player[30],
                'Pace':         player[33],
                'PacePer40':    player[34],
                'PIE':          player[36],
                'POSS':         player[37],
                'FGM':          player[38],
                'FGA':          player[39],
                'FG%':          player[42],
            }
            result_dicts.append(player)
        return(result_dicts)
    
    def measure_misc(self):
        '''measure_misc(self)
    ===
    When MeasureType == 'Misc', format results
        '''
        result_dicts = []
        for player in self.results['rowSet']:
            self._match_game(player=player)
            player = {
                'SeasonID':             self.SeasonID,
                'GameID':               self.GameID,
                'TeamID':               self.TeamID,
                'MatchupID':            self.MatchupID,
                'PlayerID':             player[0],
                'PtsTurnover':          int(player[11]),
                'PtsSecondChance':      int(player[12]),
                'PtsFastBreak':         int(player[13]),
                'PtsInThePaint':        int(player[14]),
                'OpPtsTurnover':        int(player[15]),
                'OpPtsSecondChance':    int(player[16]),
                'OpPtsFastBreak':       int(player[17]),
                'OpPtsInThePaint':      int(player[18])
            }
            result_dicts.append(player)
        return result_dicts

    def measure_usage(self):
        '''measure_usage(self)
    ===
    When MeasureType == 'Usage', format results
        '''
        result_dicts = []
        for player in self.results['rowSet']:
            self._match_game(player)
            player = {
                'SeasonID':             self.SeasonID,
                'GameID':               self.GameID,
                'TeamID':               self.TeamID,
                'MatchupID':            self.MatchupID,
                'PlayerID':             player[0],
                'Usage%':               player[11],
                '%TeamFGM':             player[12],
                '%TeamFGA':             player[13],
                '%TeamFG3M':            player[14],
                '%TeamFG3A':            player[15],
                '%TeamFTM':             player[16],
                '%TeamFTA':             player[17],
                '%TeamOREB':            player[18],
                '%TeamDREB':            player[19],
                '%TeamREB':             player[20],
                '%TeamAST':             player[21],
                '%TeamTOV':             player[22],
                '%TeamSTL':             player[23],
                '%TeamBLK':             player[24],
                '%TeamBLKd':            player[25],
                '%TeamPF':              player[26],
                '%TeamPFDrwn':          player[27],
                '%TeamPTS':             player[28],
                
            }
            '''print(f'create table {self.pipeline.schema}.PlayerBox(')
            print('SeasonID          int,')
            print('GameID            int,')
            print('TeamID            int,')
            print('MatchupID         int,')
            print('PlayerID          int,')
            for column in player.keys():
                print(f'{column}          decimal(18,3),')'''
            result_dicts.append(player)

        return(result_dicts)
    
    

    def _match_game(self, player: list):
        '''_match_game(self, player)
    ===
    Using the self.games_on_date value, finds the GameIDs and PlayerBox entries for all games that took place on that date in question.

    For each game taking place on the date in question, check for the **player** parameter, PlayerID, in home_players and away_players. 

    If found, sets **SeasonID**, **GameID**, **self.TeamID**, **self.MatchupID**    

        :param player list: list of values returned from the API for a single player
        '''
        self.matching_game = next((
            game for game in self.games_on_date 
                if player[0] in game['home_players']
                or player[0] in game['away_players']), {})
        self.SeasonID = self.matching_game.get('SeasonID')
        self.GameID = self.matching_game.get('GameID')
        if player[0] in self.matching_game['home_players']:
            self.TeamID = self.matching_game['HomeID']
            self.MatchupID = self.matching_game['AwayID']
        elif player[0] in self.matching_game['away_players']:
            self.TeamID = self.matching_game['AwayID']
            self.MatchupID = self.matching_game['HomeID']            
        else:
            self.MatchupID = 0