import logging
from datetime import datetime

from sqlalchemy import schema

class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        self.data = pipeline.data if pipeline.data else None
        pass


    def start_transform(self, data_extract):
        self.data_extract = data_extract
        if self.data and self.data != []:
            self.games_on_date = self.data if self.data else []
            self.logger.info(f'{len(self.games_on_date)} games found on {self.pipeline.date}')
        if len(self.data_extract['resultSets']) > 1:
            self.logger.warning(f'Multiple result sets returned! Only configured to handle one!')
        self.results = self.data_extract['resultSets'][0]
        self.logger.info(f'Formatting data for {self.pipeline.schema}.{self.pipeline.player_team}{self.pipeline.table_base_name}')
        if self.pipeline.schema == 'adv':
            data_transformed = self.measure_player_advanced() if self.pipeline.player_team == 'Player' else self.measure_team_advanced()
        elif self.pipeline.schema == 'misc':
            data_transformed = self.measure_player_misc() if self.pipeline.player_team == 'Player' else self.measure_team_misc()
        elif self.pipeline.schema == 'usage':
            data_transformed = self.measure_player_usage()
        elif self.pipeline.schema == 'ffactors':
            data_transformed = self.measure_team_four_factors()
        elif self.pipeline.schema == 'def':
            data_transformed = self.measure_player_defensive() if self.pipeline.player_team == 'Player' else self.measure_team_defensive()
        elif self.pipeline.schema == 'violations':
            data_transformed = self.measure_player_violations() if self.pipeline.player_team == 'Player' else self.measure_team_violations()

        elif self.pipeline.schema == 'tracking':
            data_transformed = self.transform_tracking()
        elif self.pipeline.schema == 'plays':
            data_transformed = self.transform_play_types()
        return data_transformed




#region MeasureTypes

    #region Advanced
    def measure_team_advanced(self):
        '''measure_team_advanced(self)
    ===
        <hr>
        
    When `MeasureType` API parameter == '**Advanced**', format Team results'''
        result_dicts = []
        for team in self.results['rowSet']:
            self._match_team_game(team=team)
            team = {
                'SeasonID':     self.SeasonID,
                'GameID':       self.GameID,
                'TeamID':       self.TeamID,
                'MatchupID':    self.MatchupID,
                'OffRTG':       team[8],
                'DefRTG':       team[10],
                'NetRTG':       team[12],
                'Ast%':         team[13],
                'ATR':          team[14],
                'AstRatio':     team[15],
                'OReb%':        team[16],
                'DReb%':        team[17],
                'Reb%':         team[18],
                'TeamTOV%':      team[19],
                'EOffRTG':       team[7],
                'EDefRTG':       team[9],
                'ENetRTG':       team[11],
                'EFG%':         team[20],
                'TS%':          team[21],
                'Pace':         team[23],
                'PacePer40':    team[24],
                'PIE':          team[26],
                'Possessions':         team[25],
            }
            result_dicts.append(team)
        # self._print_table_creates(dictionary=team)
        return(result_dicts)
    

    def measure_player_advanced(self):
        '''measure_player_advanced(self)
    ===
        <hr>
        
    When `MeasureType` API parameter == '**Advanced**', format Player results'''
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
                'TeamTOV%':      player[26],
                'EFG%':         player[28],
                'TS%':          player[29],
                'Usage%':       player[30],
                'Pace':         player[33],
                'PacePer40':    player[34],
                'PIE':          player[36],
                'Possessions':  player[37],
                'FGM':          player[38],
                'FGA':          player[39],
                'FG%':          player[42],
            }
            result_dicts.append(player)
        return(result_dicts)    
    #endregion Advanced


    #region Misc
    def measure_team_misc(self):
        '''measure_team_misc(self)
    ===
        <hr>
        
    When `MeasureType` API parameter == '**Misc**', format Team results'''
        result_dicts = []
        for team in self.results['rowSet']:
            self._match_team_game(team=team)
            team = {
                'SeasonID':     self.SeasonID,
                'GameID':       self.GameID,
                'TeamID':       self.TeamID,
                'MatchupID':    self.MatchupID,
                'PtsTurnover':          int(team[7]),
                'PtsSecondChance':      int(team[8]),
                'PtsFastBreak':         int(team[9]),
                'PtsInThePaint':        int(team[10]),
                'OpPtsTurnover':        int(team[11]),
                'OpPtsSecondChance':    int(team[12]),
                'OpPtsFastBreak':       int(team[13]),
                'OpPtsInThePaint':      int(team[14])
            }
            result_dicts.append(team)

        # self._print_columns_for_naming()
        # self._print_table_creates(dictionary=team)
        return(result_dicts)
    

    def measure_player_misc(self):
        '''measure_player_misc(self)
    ===
        <hr>
        
    When `MeasureType` API parameter == '**Misc**', format Player results'''
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
    #endregion Misc


    #region Usage/Four Factors
    def measure_player_usage(self):
        '''measure_usage(self)
    ===
        <hr>

    When `MeasureType` API parameter == '**Usage**', format Player results

    **Only valid parameter for Player data**'''
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
    
    def measure_team_four_factors(self):
        '''measure_team_usage(self)
    ===
        <hr>

    When `MeasureType` API parameter == '**Four Factors**', format Team results

    **Only valid parameter for Team data**'''
        result_dicts = []
        for team in self.results['rowSet']:
            self._match_team_game(team=team)
            team = {
                'SeasonID':     self.SeasonID,
                'GameID':       self.GameID,
                'TeamID':       self.TeamID,
                'MatchupID':    self.MatchupID,
                'EFG%':         team[7],
                'FTARate':      team[8],
                'TOV%':         team[9],
                'OReb%':        team[10],
                'OpEFG%':       team[11],
                'OpFTARate':    team[12],
                'OpTOV%':       team[13],
                'OpOReb%':      team[14],
            }
            result_dicts.append(team)

        # self._print_table_creates(dictionary=team)
        return(result_dicts)
    
    #endregion Usage/Four Factors


    #region Defensive
    def measure_team_defensive(self):
        '''measure_team_defensive(self)
    ===
        <hr>

    When `MeasureType` API parameter == '**Defensive**', format Team results'''
        result_dicts = []
        for team in self.results['rowSet']:
            self._match_team_game(team=team)
            team = {
                'SeasonID':     self.SeasonID,
                'GameID':       self.GameID,
                'TeamID':       self.TeamID,
                'MatchupID':    self.MatchupID,
                'DReb%':        team[9],
            }
            result_dicts.append(team)

        # self._print_columns_for_naming()
        # self._print_table_creates(dictionary=team)
        return(result_dicts)
    
    def measure_player_defensive(self):
        '''measure_defensive(self)
    ===
        <hr>
        
    When `MeasureType` API parameter == '**Defensive**', format Player results'''
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
    #endregion Defensive


    #region Violations
    def measure_team_violations(self):
        '''measure_team_violations(self)
    ===
        <hr>
        
    When `MeasureType` API parameter == '**Violations**', format Team results'''
        result_dicts = []
        for team in self.results['rowSet']:
            self._match_team_game(team=team)
            team = {
                'SeasonID':     self.SeasonID,
                'GameID':       self.GameID,
                'TeamID':       self.TeamID,
                'MatchupID':    self.MatchupID,
                'Travel':       team[6],
                'DblDribble':   team[7],
                'Inbound':      team[13],
                'Backcourt':    team[14],
                'Palming':      team[16],
                'OffFoul':      team[17],
                'Off3':         team[9],
                'OffGoaltend':  team[15],
                'Def3':         team[18],
                'DefGoaltend':  team[21],
                'FiveSec':      team[10],
                'EightSec':     team[11],
                'ShotClock':    team[12],
                'Charge':       team[19],
                'DelayOfGame':  team[20],
                'Lane':         team[22],
                'JumpBall':     team[23],
                'KickedBall':   team[24],
                'DiscDribble':  team[8],
            }
            result_dicts.append(team)

        # self._print_columns_for_naming()
        self._print_table_creates(dictionary=team)
        return(result_dicts)
    
    def measure_player_violations(self):
        '''measure_violations(self)
    ===
        <hr>

    When `MeasureType` API parameter == '**Violations**', format Player results'''
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
    #endregion Violations  

#endregion MeasureTypes





#region Utility - Game Match
    def _set_result_formatted(self, result: list):
        '''_set_result_formatted(self, result)
    ===
        <hr>

    Determines **SeasonID**, **GameID**, **sf.TeamID** and **MatchupID** values for result_formatted. 
    
    If *self.pipeline.player_team* == 'Player', sets **PlayerID** for result_formatted. 

    Parameters
    ---

        __result__ (list): list of values for a particular Player or Team

    Function Calls
    ---

        self._match_team_game(result)'''
        self.index_diff = 0
        if self.pipeline.player_team == 'Team':
            self._match_team_game(team=result)
            self.result_formatted = {
                'SeasonID':     self.SeasonID,
                'GameID':       self.GameID,
                'TeamID':       self.TeamID,
                'MatchupID':    self.MatchupID,
            }            
            self.logger.info(f'Key values set! SeasonID: {self.SeasonID}, GameID: {self.GameID}, TeamID: {self.TeamID}, MatchupID: {self.MatchupID}')
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
            self.logger.info(f'Key values set! SeasonID: {self.SeasonID}, GameID: {self.GameID}, TeamID: {self.TeamID}, MatchupID: {self.MatchupID}, PlayerID: {result[0]}')


    def _match_player_game(self, player: list):
        '''_match_player_game(self, player)
    ===
        <hr>

    Using the self.games_on_date value, finds the GameIDs and PlayerBox entries for all games that took place on that date in question.

    For each game taking place on the date in question, check for the **player** parameter, PlayerID, in home_players and away_players. 

    If found, sets **SeasonID**, **GameID**, **self.TeamID**, **self.MatchupID**    

    Parameters
    -------------
    <hr>

        __player__ list: list of values returned from the API for a single player'''
        self.logger.info(f'Finding game on {self.pipeline.date} with {player[1]} in the lineup...')
        self.matching_game = next((
            game for game in self.games_on_date 
                if player[0] in game['home_players']
                or player[0] in game['away_players']), {})
        self.SeasonID = self.matching_game.get('SeasonID')
        self.GameID = self.matching_game.get('GameID')
        self.logger.info(f'Game found! {self.GameID}')
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
        <hr>

    Using the self.games_on_date value, finds the GameIDs and their respective HomeID and AwayID entries for all games that took place on that date in question.

    For each game taking place on the date in question, check if the **team** parameter, TeamID, is equal to HomeID or AwayID

    If found, sets **SeasonID**, **GameID**, **self.TeamID**, **self.MatchupID**    

    Parameters
    -------------
    <hr>
        
        __team__ (list): list of values returned from the API for a single Team'''
        team_name = team[2] if type(team[2]) == str else team[1]
        self.logger.info(f'Finding game on {self.pipeline.date} with the {team_name}...')
        self.matching_game = next((
            game for game in self.games_on_date 
                if team[0] == game['HomeID']
                or team[0] == game['AwayID']), {})
        self.SeasonID = self.matching_game.get('SeasonID')
        self.GameID = self.matching_game.get('GameID')
        self.logger.info(f'Game found! {self.GameID}')
        if team[0] == self.matching_game['HomeID']:
            self.TeamID = self.matching_game['HomeID']
            self.MatchupID = self.matching_game['AwayID']
        elif team[0] == self.matching_game['AwayID']:
            self.TeamID = self.matching_game['AwayID']
            self.MatchupID = self.matching_game['HomeID']            
        else:
            self.logger.critical(f'RARE ERROR!!! Match not found!')
            self.MatchupID = 0

#endregion Utility - Game Match



#region PlayTypes



    def transform_play_types(self):
        '''transform_play_types`(self)`
    ===
        <hr>

    Acts similarly to `transform_tracking`, but for the Synergy PlayType endpoint

    Function Calls
    -------------
    <hr>

    *   **_format_team_player_constants(result)**
        - Drives formatting of Team, TeamBox, Player, PlayerBox and StartingLineups dicts and lists of dicts

    *   **parse_plays(result)**
        - Parses and formats results from Synergy API


    Returns
    -------------
    <hr>

    __data_transformed__ (list): Transformed PlayType data ready for load to plays.*PlayType* table'''
        self.result_dicts = []
        for result in self.results['rowSet']:
            self._format_team_player_constants(result=result)
            self.result_dicts.append(self.parse_plays(result=result))
        bp = 'here'
        data_transformed = self.result_dicts
        return data_transformed


    def parse_plays(self, result: list):
        '''parse_plays`(self, result)`
    ===
        <hr>

    Parses and formats results from NBA/Synergy API<br>
    Works for all PlayTypes and if Offensive/Defensive.


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **Isolation** play type data'''
        result_dict = {
            **self.result_formatted,
            'Play': result[4 + self.index_diff],
            'Type': result[5 + self.index_diff],
            'GP': result[7 + self.index_diff],
            'Possessions': result[17 + self.index_diff],
            'Frequency': result[8 + self.index_diff],
            'PtsPerPoss': result[9 + self.index_diff],
            'FGM': result[19 + self.index_diff],
            'FGA': result[20 + self.index_diff],
            'PTS': result[18 + self.index_diff],
            'FG%': result[10 + self.index_diff],
            'EFG%': result[16 + self.index_diff],
            'FTFreq': result[11 + self.index_diff],
            'TOVFreq': result[12 + self.index_diff],
            'FDrwnFreq': result[13 + self.index_diff],
            'And1Freq': result[14 + self.index_diff],
            'ScoreFreq': result[15 + self.index_diff],
            'FGMX': result[21 + self.index_diff],            
            'Percentile': result[6 + self.index_diff],
            'FirstDate': datetime.now(),
            'LastDate': datetime.now()
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        bp = 'here'
        return result_dict
    




    #region Utility - Constants
    def _format_team_player_constants(self, result: list):
        '''_format_team_player_constants`(self)`
    ===
        <hr>

    Since the Synergy Playtypes don't use date tracking, we can't match GameID. 

    Differing from tracking, we'll set our `index_diff` equal to 2 if `self.pipeline.player_team` == 'Player'

    <hr>

    Sets *`self.result_formatted`*
    ---
    - Team: **SeasonID**, **TeamID**        
    - Player: **SeasonID**, **TeamID**, **PlayerID**'''
        if self.pipeline.player_team == 'Team':
            self.index_diff = 0
            self.result_formatted = {
                'SeasonID': result[0][1:],
                'TeamID': result[1],
            }
        elif self.pipeline.player_team == 'Player':
            self.index_diff = 2
            self.result_formatted = {
                'SeasonID': result[0][1:],
                'TeamID': result[3],
                'PlayerID': result[1]
            }
    #endregion Utility - Constants

#endregion PlayTypes

#region Tracking
    def transform_tracking(self):
        '''transform_tracking`(self)`
    ===
        <hr>    

    Acts similarly to `start_transform`, but for the Tracking endpoint'''
        self.result_dicts = []
        for result in self.results['rowSet']:
            self._set_result_formatted(result=result)

            if self.pipeline.table_base_name == 'Drives':
                self.result_dicts.append(self.tracking_drives(result=result))
            elif self.pipeline.table_base_name == 'Defense':
                self.result_dicts.append(self.tracking_defensive_impact(result=result))
            elif self.pipeline.table_base_name == 'CatchShoot':
                self.result_dicts.append(self.tracking_catch_shoot(result=result))
            elif self.pipeline.table_base_name == 'Passing':
                self.result_dicts.append(self.tracking_passing(result=result))
            elif self.pipeline.table_base_name == 'Possessions':
                self.result_dicts.append(self.tracking_possessions(result=result))
            elif self.pipeline.table_base_name == 'PullUpShot':
                self.result_dicts.append(self.tracking_shot_pull_up(result=result))
            elif self.pipeline.table_base_name == 'Rebounding':
                self.result_dicts.append(self.tracking_rebounding(result=result))
            elif self.pipeline.table_base_name == 'Efficiency':
                self.result_dicts.append(self.tracking_efficiency(result=result))
            elif self.pipeline.table_base_name == 'SpeedDistance':
                self.result_dicts.append(self.tracking_speed_distance(result=result))
            elif self.pipeline.table_base_name == 'ElbowTouch':
                self.result_dicts.append(self.tracking_touch_elbow(result=result))
            elif self.pipeline.table_base_name == 'PostTouch':
                self.result_dicts.append(self.tracking_touch_post(result=result))
            elif self.pipeline.table_base_name == 'PaintTouch':
                self.result_dicts.append(self.tracking_touch_paint(result=result))
                
            elif self.pipeline.table_base_name == 'Hustle':
                if self.pipeline.full_table_name == 'TeamHustle':
                    self.result_dicts.append(self.tracking_team_hustle(result=result))
                elif self.pipeline.full_table_name == 'PlayerHustle':
                    self.result_dicts.append(self.tracking_player_hustle(result=result))
            self.logger.info(f'Result formatted succesfully')
        data_transformed = self.result_dicts
        return data_transformed


    def tracking_drives(self, result: list):
        '''tracking_drives`(self, result)`
    ===
        <hr>
    
    When `PtMeasureType` == '**Drives'**', format results<br>
    Depending on whether or not player_team is Player or Team, format dictionaries accordingly for Drive data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **Drives** tracking data'''
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
        <hr>
    
    When `PtMeasureType` == '**Defense**', format results<br>
    Depending on whether or not player_team is Player or Team, format dictionaries accordingly for Defensive Impact data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **Defended Field Goal** tracking data'''
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
        <hr>
    
    When `PtMeasureType` == '**CatchShoot**', format results<br>
    Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **CatchShoot** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **CatchShoot** tracking data'''
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
        <hr>
    
    When `PtMeasureType` == '**Passing**', format results<br>
    Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **Passing** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **Passing** tracking data'''
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
        <hr>
    
    When `PtMeasureType` == '**Possessions**', format results<br>
    Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **Possessions** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **Possessions** tracking data'''
        result_dict = {
            **self.result_formatted,
            'Pts': result[7 + self.index_diff],
            'Touches': result[8 + self.index_diff],
            'TouchFrontCourt': result[9 + self.index_diff],
            'PossTime': result[10 + self.index_diff],
            'SecPerTouch': result[11 + self.index_diff],
            'DribblePerTouch': result[12 + self.index_diff],
            'PtsPerTouch': result[13 + self.index_diff],
            'TouchElbow': result[14 + self.index_diff],
            'TouchPost': result[15 + self.index_diff],
            'TouchPaint': result[16 + self.index_diff],
            'PtsPerElbowTouch': result[17 + self.index_diff],
            'PtsPerPostTouch': result[18 + self.index_diff],
            'PtsPerPaintTouch': result[19 + self.index_diff],
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict

    def tracking_shot_pull_up(self, result:list):
        '''tracking_shot_pull_up`(self, result)`
    ===
        <hr>
    
    When `PtMeasureType` == '**PullUpShot**', format results<br>
    Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **PullUpShot** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **PullUpShot** tracking data'''
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

    def tracking_rebounding(self, result:list):
        '''tracking_rebounding`(self, result)`
    ===
        <hr>
        
    When `PtMeasureType` == '**Rebounding**', format results<br>
    Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **Rebounding** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **Rebounding** tracking data'''
        result_dict = {
            **self.result_formatted,
            'OReb': result[7 + self.index_diff],
            'ORebContested': result[8 + self.index_diff],
            'ORebUnContested': result[9 + self.index_diff],
            'ORebChances': result[11 + self.index_diff],
            'ORebChanceDefer': result[13 + self.index_diff],
            'AvgORebDist': result[15 + self.index_diff],
            'DReb': result[16 + self.index_diff],
            'DRebContested': result[17 + self.index_diff],
            'DRebUnContested': result[18 + self.index_diff],
            'DRebChances': result[20 + self.index_diff],
            'DRebChanceDefer': result[22 + self.index_diff],
            'AvgDRebDist': result[24 + self.index_diff],
            'Reb': result[25 + self.index_diff],
            'RebContested': result[26 + self.index_diff],
            'RebUnContested': result[27 + self.index_diff],
            'RebChances': result[29 + self.index_diff],
            'RebChanceDefer': result[31 + self.index_diff],
            'AvgRebDist': result[33 + self.index_diff],
            'ORebContested%': result[10 + self.index_diff],
            'ORebChance%': result[12 + self.index_diff],
            'ORebChanceAdj%': result[14 + self.index_diff],
            'DRebContested%': result[19 + self.index_diff],
            'DRebChance%': result[21 + self.index_diff],
            'DRebChanceAdj%': result[23 + self.index_diff],
            'RebContested%': result[28 + self.index_diff],
            'RebChance%': result[30 + self.index_diff],
            'RebChanceAdj%': result[32 + self.index_diff],
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict

    def tracking_efficiency(self, result:list):
        '''tracking_efficiency`(self, result)`
    ===
        <hr>
    
    When `PtMeasureType` == '**Efficiency**', format results<br>
    Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **Efficiency** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **Efficiency** tracking data'''
        result_dict = {
            **self.result_formatted,
            'DrivePts': result[8 + self.index_diff],
            'CatchShootPts': result[10 + self.index_diff],
            'PullUpPts': result[12 + self.index_diff],
            'PaintTouchPts': result[14 + self.index_diff],
            'PostTouchPts': result[16 + self.index_diff],
            'ElbowTouchPts': result[18 + self.index_diff],
            'DriveFG%': result[9 + self.index_diff],
            'CatchShootFG%': result[11 + self.index_diff],
            'PullUpFG%': result[13 + self.index_diff],
            'PaintTouchFG%': result[15 + self.index_diff],
            'PostTouchFG%': result[17 + self.index_diff],
            'ElbowTouchFG%': result[19 + self.index_diff],
            'EFG%': result[20 + self.index_diff],
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict

    def tracking_speed_distance(self, result:list):
        '''tracking_speed_distance`(self, result)`
    ===
        <hr>

    When `PtMeasureType` == '**SpeedDistance**', format results<br>
    Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **SpeedDistance** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **SpeedDistance** tracking data'''
        result_dict = {
            **self.result_formatted,
            'DistFeet': result[8 + self.index_diff],
            'DistMiles': result[9 + self.index_diff],
            'DistMilesOff': result[10 + self.index_diff],
            'DistMilesDef': result[11 + self.index_diff],
            'AvgSpeed': result[12 + self.index_diff],
            'AvgSpeedOff': result[13 + self.index_diff],
            'AvgSpeedDef': result[14 + self.index_diff],
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict

    def tracking_touch_elbow(self, result:list):
        '''tracking_touch_elbow`(self, result)`
    ===
        <hr>
    
    When `PtMeasureType` == '**ElbowTouch**', format results<br>
    Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **ElbowTouch** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **ElbowTouch** tracking data'''
        result_dict = {
            **self.result_formatted,
            'TotalTouches': result[7 + self.index_diff],
            'ElbowTouches': result[8 + self.index_diff],
            'FGM': result[9 + self.index_diff],
            'FGA': result[10 + self.index_diff],
            'FG%': result[11 + self.index_diff],
            'FTM': result[12 + self.index_diff],
            'FTA': result[13 + self.index_diff],
            'FT%': result[14 + self.index_diff],
            'Pts': result[15 + self.index_diff],
            'Passes': result[16 + self.index_diff],
            'Ast': result[17 + self.index_diff],
            'TOV': result[19 + self.index_diff],
            'Fouls': result[21 + self.index_diff],
            '%ofPts': result[24 + self.index_diff],
            'Pass%': result[22 + self.index_diff],
            'Ast%': result[18 + self.index_diff],
            'TOV%': result[20 + self.index_diff],
            'Foul%': result[23 + self.index_diff],
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict

    def tracking_touch_post(self, result:list):
        '''tracking_touch_post`(self, result)`
    ===
        <hr>
    
    When `PtMeasureType` == '**PostTouch**', format results<br>
    Depending on whether or not player_team is Player or Team, format dictionaries accordingly for **PostTouch** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **PostTouch** tracking data'''
        result_dict = {
            **self.result_formatted,
            'TotalTouches': result[7 + self.index_diff],
            'PostTouches': result[8 + self.index_diff],
            'FGM': result[9 + self.index_diff],
            'FGA': result[10 + self.index_diff],
            'FG%': result[11 + self.index_diff],
            'FTM': result[12 + self.index_diff],
            'FTA': result[13 + self.index_diff],
            'FT%': result[14 + self.index_diff],
            'Pts': result[15 + self.index_diff],
            'Passes': result[17 + self.index_diff],
            'Ast': result[19 + self.index_diff],
            'TOV': result[21 + self.index_diff],
            'Fouls': result[23 + self.index_diff],
            '%ofPts': result[16 + self.index_diff],
            'Pass%': result[18 + self.index_diff],
            'Ast%': result[20 + self.index_diff],
            'TOV%': result[22 + self.index_diff],
            'Foul%': result[24 + self.index_diff],
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict

    def tracking_touch_paint(self, result:list):
        '''tracking_touch_paint`(self, result)`
    ===
        <hr>
    
    When `PtMeasureType` == '**PaintTouch**', format results<br>
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
            'TotalTouches': result[7 + self.index_diff],
            'PaintTouches': result[8 + self.index_diff],
            'FGM': result[9 + self.index_diff],
            'FGA': result[10 + self.index_diff],
            'FG%': result[11 + self.index_diff],
            'FTM': result[12 + self.index_diff],
            'FTA': result[13 + self.index_diff],
            'FT%': result[14 + self.index_diff],
            'Pts': result[15 + self.index_diff],
            'Passes': result[17 + self.index_diff],
            'Ast': result[19 + self.index_diff],
            'TOV': result[21 + self.index_diff],
            'Fouls': result[23 + self.index_diff],
            '%ofPts': result[16 + self.index_diff],
            'Pass%': result[18 + self.index_diff],
            'Ast%': result[20 + self.index_diff],
            'TOV%': result[22 + self.index_diff],
            'Foul%': result[24 + self.index_diff],
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict

#endregion Tracking



#region Hustle
    def tracking_team_hustle(self, result:list):
        '''tracking_hustle`(self, result)`
    ===
        <hr>
        
    Handles response from league Hustle endpoint

    Depending on whether or not we hit the team or player version of the endpoint, format dictionaries accordingly for **PaintTouch** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **Hustle** data'''
        result_dict = {
            **self.result_formatted,
            'ContestedShots': result[3 + self.index_diff],
            'ContestedFG2': result[4 + self.index_diff],
            'ContestedFG3': result[5 + self.index_diff],
            'Deflections': result[6 + self.index_diff],
            'ChargesDrawn': result[7 + self.index_diff],
            'ScreenAst': result[8 + self.index_diff],
            'ScreenAstPts': result[9 + self.index_diff],
            'OffLooseBallsRec': result[10 + self.index_diff],
            'DefLooseBallsRec': result[11 + self.index_diff],
            'LooseBallsRec': result[12 + self.index_diff],
            'OffBoxouts': result[15 + self.index_diff],
            'DefBoxouts': result[16 + self.index_diff],
            'Boxouts': result[17 + self.index_diff],
            'LooseBallsRecOff%': result[13 + self.index_diff],
            'LooseBallsRecDef%': result[14 + self.index_diff],
            'OffBoxout%': result[18 + self.index_diff],
            'DefBoxout%': result[19 + self.index_diff],
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict

    def tracking_player_hustle(self, result:list):
        '''tracking_player_hustle`(self, result)`
    ===
        <hr>
        
    Handles response from league Hustle endpoint

    Depending on whether or not we hit the team or player version of the endpoint, format dictionaries accordingly for **PaintTouch** data


    Parameters
    -------------
    <hr>

        __result__ (list): A list of either Player or Team data for a single Date


    Returns
    -------------
    <hr>

        __result_dict__ (dict): Formatted dict of **Hustle** data'''
        result_dict = {
            **self.result_formatted,
            'ContestedShots': result[6 + self.index_diff],
            'ContestedFG2': result[7 + self.index_diff],
            'ContestedFG3': result[8 + self.index_diff],
            'Deflections': result[9 + self.index_diff],
            'ChargesDrawn': result[10 + self.index_diff],
            'ScreenAst': result[11 + self.index_diff],
            'ScreenAstPts': result[12 + self.index_diff],
            'OffLooseBallsRec': result[13 + self.index_diff],
            'DefLooseBallsRec': result[14 + self.index_diff],
            'LooseBallsRec': result[15 + self.index_diff],
            'OffBoxouts': result[18 + self.index_diff],
            'DefBoxouts': result[19 + self.index_diff],
            'Boxouts': result[20 + self.index_diff],
            'BoxoutTeamReb': result[21 + self.index_diff],
            'BoxoutPersReb': result[22 + self.index_diff],
            'LooseBallsRecOff%': result[16 + self.index_diff],
            'LooseBallsRecDef%': result[17 + self.index_diff],
            'OffBoxout%': result[23 + self.index_diff],
            'DefBoxout%': result[24 + self.index_diff],
            'BoxoutTeamReb%': result[25 + self.index_diff],
            'BoxoutPersReb%': result[26 + self.index_diff],
        }
        # self._print_columns_for_naming()
        # self._print_table_creates(result_dict)
        return result_dict

#endregion Hustle






#region Helper - result printing
    def _print_columns_for_naming(self):
        for i, column in enumerate(self.results['headers']):
            i_nbr = i if self.pipeline.player_team == 'Team' else i - 1
            print(f"            '{column}': result[{i_nbr} + self.index_diff],")
        bp = 'here'

    def _print_table_creates(self, dictionary):
        # import pyperclip
        check_string = f"""if not exists(
select *
from sys.schemas s
where s.name = '{self.pipeline.schema}'
)
exec('create schema {self.pipeline.schema}');
if not exists(
select *
from sys.tables t
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = '{self.pipeline.table_name}' and s.name = '{self.pipeline.schema}'
)
begin"""
        check_string += f'\ncreate table {self.pipeline.full_table_name}('
        full_str = check_string
        print(check_string)
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
            

        for column in dictionary.keys():
            if '%' in column:
                col = f'[{column}]'
                spacer = f'{(18 - len(col)) * ' '}'
                col_string = f'[{column}]{spacer}decimal(18,3),'
            elif 'RTG' in column:
                col = f'{column}'
                spacer = f'{(18 - len(col)) * ' '}'
                col_string = f'{column}{spacer}decimal(18,1),'
            else:
                spacer = f'{(18 - len(column)) * ' '}'
                col_string = f'{column}{spacer}int,'
            
            full_str += f'\n{col_string}'
            print(col_string)
        full_str += f'\n{key_string}'
        print(key_string)
        # pyperclip.copy(full_str)

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
