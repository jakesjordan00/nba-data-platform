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
        elif self.pipeline.schema == 'tracking':
            data_transformed = self.transform_tracking()
        return data_transformed

    def measure_violations(self):
        '''measure_violations(self)
    ===
    When MeasureType == 'Violations', format results
        '''
        result_dicts = []
        for player in self.results['rowSet']:
            self._match_player_game(player=player)
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
        result_dicts = []
        for player in self.results['rowSet']:
            self._match_player_game(player=player)
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
            self._match_player_game(player=player)
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
            self._match_player_game(player)
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
            result_dicts.append(player)
        return(result_dicts)
    
    
    def measure_defensive(self):
        '''measure_defensive(self)
    ===
    When MeasureType == 'Defensive', format results
        '''
        result_dicts = []
        for player in self.results['rowSet']:
            self._match_player_game(player=player)
            player = {
                'SeasonID':     self.SeasonID,
                'GameID':       self.GameID,
                'TeamID':       self.TeamID,
                'MatchupID':    self.MatchupID,
                'PlayerID':     player[0],
                'DReb%': player[13],
                '%TeamDReb': player[14],
                '%TeamSTL': player[16],
                '%TeamBLK': player[18],
                'DefWinShare': player[23],
            }
            result_dicts.append(player)
            
        return result_dicts
    




    def _match_player_game(self, player: list):
        '''_match_player_game(self, player)
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

    def _match_team_game(self, team: list):
        '''_match_team_game(self, team)
    ===
    Using the self.games_on_date value, finds the GameIDs and their respective HomeID and AwayID entries for all games that took place on that date in question.

    For each game taking place on the date in question, check if the **team** parameter, TeamID, is equal to HomeID or AwayID

    If found, sets **SeasonID**, **GameID**, **self.TeamID**, **self.MatchupID**    

        :param team list: list of values returned from the API for a single Team

        '''
        self.matching_game = next((
            game for game in self.games_on_date 
                if team[0] == game['HomeID']
                or team[0] == game['AwayID']), {})
        self.SeasonID = self.matching_game.get('SeasonID')
        self.GameID = self.matching_game.get('GameID')
        if team[0] == self.matching_game['HomeID']:
            self.TeamID = self.matching_game['HomeID']
            self.MatchupID = self.matching_game['AwayID']
        elif team[0] == self.matching_game['AwayID']:
            self.TeamID = self.matching_game['AwayID']
            self.MatchupID = self.matching_game['HomeID']            
        else:
            self.MatchupID = 0



#region Tracking
    def transform_tracking(self):
        '''transform_tracking`(self)`
    ===

        Acts similarly to `start_transform`, but for the Tracking endpoint
        '''
        self.result_dicts = []
        for result in self.results['rowSet']:
            self.index_diff = 0
            if self.pipeline.player_team == 'Team':
                self._match_team_game(team=result)
                self.result_formatted = {
                    'SeasonID': self.SeasonID,
                    'GameID': self.GameID,
                    'TeamID':       self.TeamID,
                    'MatchupID':    self.MatchupID,
                }
            elif self.pipeline.player_team == 'Player':
                self._match_player_game(player=result)
                self.index_diff = 1
                self.result_formatted = {
                    'SeasonID':     self.SeasonID,
                    'GameID':       self.GameID,
                    'TeamID':       self.TeamID,
                    'MatchupID':    self.MatchupID,
                    'PlayerID':     result[0],
                }

            if self.pipeline.tracking_table == 'Drives':
                self.result_dicts.append(self.tracking_drives(result=result))
            elif self.pipeline.tracking_table == 'Defense':
                self.result_dicts.append(self.tracking_defensive_impact(result=result))
            elif self.pipeline.tracking_table == 'CatchShoot':
                self.result_dicts.append(self.tracking_catch_shoot(result=result))
            elif self.pipeline.tracking_table == 'Passing':
                self.result_dicts.append(self.tracking_passing(result=result))
            elif self.pipeline.tracking_table == 'Possessions':
                self.result_dicts.append(self.tracking_possessions(result=result))

            elif self.pipeline.tracking_table == 'PullUpShot':
                self.result_dicts.append(self.tracking_shot_pull_up(result=result))
            elif self.pipeline.tracking_table == 'Rebounding':
                self.result_dicts.append(self.tracking_rebounding(result=result))
            elif self.pipeline.tracking_table == 'Efficiency':
                self.result_dicts.append(self.tracking_efficiency(result=result))
            elif self.pipeline.tracking_table == 'SpeedDistance':
                self.result_dicts.append(self.tracking_speed_distance(result=result))
            elif self.pipeline.tracking_table == 'ElbowTouch':
                self.result_dicts.append(self.tracking_touch_elbow(result=result))
            elif self.pipeline.tracking_table == 'PostTouch':
                self.result_dicts.append(self.tracking_touch_post(result=result))
            elif self.pipeline.tracking_table == 'PaintTouch':
                self.result_dicts.append(self.tracking_touch_paint(result=result))
            bp = 'here'
        bp = 'here'
        data_transformed = self.result_dicts
        return data_transformed


    def tracking_drives(self, result: list):
        '''tracking_drives`(self, result)`
    ===
        When PtMeasureType == 'Drives', format results<br>
        Depending on whether or not player_team is Player or Team, format dictionaries accordingly for Drive data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **Drives** tracking data
    '''
        result_dict = {
            **self.result_formatted,                
            'Drives': result[7 + self.index_diff],
            'FGM': result[8 + self.index_diff],
            'FGA': result[9 + self.index_diff],
            'FG%': result[10 + self.index_diff],
            'FTM': result[11 + self.index_diff],
            'FTA': result[12 + self.index_diff],
            'FT%': result[13 + self.index_diff],
            'PTS': result[14 + self.index_diff],
            'PTS%': result[15 + self.index_diff],
            'Passes': result[16 + self.index_diff],
            'Pass%': result[17 + self.index_diff],
            'AST': result[18 + self.index_diff],
            'AST%': result[19 + self.index_diff],
            'TOV': result[20 + self.index_diff],
            'TOV%': result[21 + self.index_diff],
            'PF': result[22 + self.index_diff],
            'PF%': result[23 + self.index_diff],
        }
        # self._print_table_creates(result)
        bp = 'here'
        return result_dict
    
    def tracking_defensive_impact(self, result: list):
        '''tracking_defensive_impact`(self, result)`
    ===
        When PtMeasureType == 'Defense', format results<br>
        Depending on whether or not player_team is Player or Team, format dictionaries accordingly for Defensive Impact data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **Defended Field Goal** tracking data
    '''
        result_dict = {
            **self.result_formatted,  
            'DefRimFGM': result[10 + self.index_diff],
            'DefRimFGA': result[11 + self.index_diff],
            'DefRimFG%': result[12 + self.index_diff],
        }
        # self._print_table_creates(result_dict)
        return result_dict

    def tracking_catch_shoot(self, result: list):
        '''tracking_catch_shoot`(self, result)`
    ===
        When PtMeasureType == 'CatchShoot', format results<br>
        Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **CatchShoot** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **CatchShoot** tracking data
    '''
        result_dict = {
            **self.result_formatted,
            'FGM': result[7 + self.index_diff],
            'FGA': result[8 + self.index_diff],
            'FG%': result[9 + self.index_diff],
            'PTS': result[10 + self.index_diff],
            'FG3M': result[11 + self.index_diff],
            'FG3A': result[12 + self.index_diff],
            'FG3%': result[13 + self.index_diff],
            'EFG%': result[14 + self.index_diff],
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict

    def tracking_passing(self, result: list):
        '''tracking_passing`(self, result)`
    ===
        When PtMeasureType == 'Passing', format results<br>
        Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **Passing** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **Passing** tracking data
    '''
        result_dict = {
            **self.result_formatted,
            'PassMade': result[7 + self.index_diff],
            'PassRcvd': result[8 + self.index_diff],
            'Ast': result[9 + self.index_diff],
            'AstFT': result[10 + self.index_diff],
            'AstSecondary': result[11 + self.index_diff],
            'AstPotential': result[12 + self.index_diff],
            'AstPointsCreated': result[13 + self.index_diff],
            'AstAdj': result[14 + self.index_diff],
            'AstToPass%': result[15 + self.index_diff],
            'AstToPass%Adj': result[16 + self.index_diff],
        }
        # self._print_table_creates(result_dict)
        return result_dict



    def tracking_possessions(self, result: list):
        '''tracking_possessions`(self, result)`
    ===
        When PtMeasureType == 'Possessions', format results<br>
        Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **Possessions** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **Possessions** tracking data
    '''
        result_dict = {
            **self.result_formatted,
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict
    

    



    def tracking_shot_pull_up(self, result:list):
        '''tracking_shot_pull_up`(self, result)`
    ===
        When PtMeasureType == 'PullUpShot', format results<br>
        Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **PullUpShot** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **PullUpShot** tracking data
    '''
        result_dict = {
            **self.result_formatted,
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict


    def tracking_rebounding(self, result:list):
        '''tracking_rebounding`(self, result)`
    ===
        When PtMeasureType == 'Rebounding', format results<br>
        Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **Rebounding** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **Rebounding** tracking data
    '''
        result_dict = {
            **self.result_formatted,
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict


    def tracking_efficiency(self, result:list):
        '''tracking_efficiency`(self, result)`
    ===
        When PtMeasureType == 'Efficiency', format results<br>
        Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **Efficiency** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **Efficiency** tracking data
    '''
        result_dict = {
            **self.result_formatted,
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict

    def tracking_speed_distance(self, result:list):
        '''tracking_speed_distance`(self, result)`
    ===
        When PtMeasureType == 'SpeedDistance', format results<br>
        Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **SpeedDistance** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **SpeedDistance** tracking data
    '''
        result_dict = {
            **self.result_formatted,
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict

    def tracking_touch_elbow(self, result:list):
        '''tracking_touch_elbow`(self, result)`
    ===
        When PtMeasureType == 'ElbowTouch', format results<br>
        Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **ElbowTouch** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **ElbowTouch** tracking data
    '''
        result_dict = {
            **self.result_formatted,
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict

    def tracking_touch_post(self, result:list):
        '''tracking_touch_post`(self, result)`
    ===
        When PtMeasureType == 'PostTouch', format results<br>
        Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **PostTouch** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **PostTouch** tracking data
    '''
        result_dict = {
            **self.result_formatted,
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict

    def tracking_touch_paint(self, result:list):
        '''tracking_touch_paint`(self, result)`
    ===
        When PtMeasureType == 'PaintTouch', format results<br>
        Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **PaintTouch** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **PaintTouch** tracking data
    '''
        result_dict = {
            **self.result_formatted,
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict


#endregion Tracking






#region Printing Utilities/Helpers
    def _print_columns_for_naming(self):
        for i, column in enumerate(self.results['headers']):
            i_nbr = i if self.pipeline.player_team == 'Team' else i - 1
            print(f"            '{column}': result[{i_nbr} + self.index_diff],")
        bp = 'here'

    def _print_table_creates(self, dictionary):
        import pyperclip
        check_string = f"""
if not exists(
select *
from sys.schemas s
where s.name = '{self.pipeline.schema}'
)
exec('create schema {self.pipeline.schema}');
if not exists(
select *
from sys.tables t
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = '{self.pipeline.full_table_name}' and s.name = '{self.pipeline.schema}'
)
begin"""
        full_str = check_string
        if 'Team' in self.pipeline.full_table_name:
            key_string = f"""Primary Key(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID))
end"""
        elif 'Player' in self.pipeline.full_table_name:
            key_string = f"""Primary Key(SeasonID, GameID, TeamID, MatchupID, PlayerID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, PlayerID) references Player(SeasonID, PlayerID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID, PlayerID) references PlayerBox(SeasonID, GameID, TeamID, MatchupID, PlayerID))
end"""
        print(check_string)
        print(f'create table {self.pipeline.schema}.{self.pipeline.full_table_name}(')
        for column in dictionary.keys():
            if '%' in column:
                col = f'[{column}]'
                spacer = f'{(18 - len(col)) * ' '}'
                col_string = f'[{column}]{spacer}decimal(18,3),'
            else:
                spacer = f'{(18 - len(column)) * ' '}'
                col_string = f'{column}{spacer}int,'
            
            full_str += f'\n{col_string}'
            print(col_string)
        full_str += f'\n{key_string}'
        print(key_string)
        pyperclip.copy(full_str)

        print("\n\n        'columns': [")
        for column in dictionary.keys():
            if '%' in column:
                print(f"            '[{column}]',")
            else:
                print(f"            '{column}',")
        
        print("        ],")
        print("        'update_columns': [")
        for column in dictionary.keys():
            if 'ID' not in column:
                if '%' in column:
                    print(f"            '[{column}]',")
                else:
                    print(f"            '{column}',")
        print("        ],")
