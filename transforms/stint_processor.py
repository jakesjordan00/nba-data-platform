import logging
from dataclasses import dataclass
from tracemalloc import start
from typing import Any
import pandas as pd

@dataclass
class StintError:
    action: dict
    type: str
    error_msg: str
    type_detail: str | None
    team_id: int | None
    player_id: int | None
    home_stats: dict
    away_stats: dict
    last10: list

    def __init__(self, action: dict, stint_processor, error_msg: str, type: str):
        bp = 'here'
        self.action = action
        self.action_number = action['actionNumber']
        self.type = type
        self.error_msg = error_msg
        self.team_id = action.get('teamId')
        self.player_id = action.get('personId')
        self.last10 = stint_processor.playbyplay_data[action['Index']-10:action['Index']]
        self.home_stats = stint_processor.home_stats
        self.away_stats = stint_processor.away_stats
        self.type_detail = ''

@dataclass
class StintResult:
    Stint: list
    StintPlayer: list
    team_player_stints: list
    status: dict
    errors: list[StintError] | None = None

#region StintProcessor
class StintProcessor:
    def __init__(self, playbyplay_data: list, boxscore_data: dict, sub_groups: list, home_stats: dict | None, away_stats: dict | None, stint_status: str, db_actions: int = 0, db_last_action_number: int = 0, current_sub_group_index: int = 0):
        self.playbyplay_data = playbyplay_data
        self.boxscore_data = boxscore_data
        self.sub_groups = sub_groups
        self.db_actions = db_actions
        self.db_last_action_number = db_last_action_number
        self.last_action = playbyplay_data[-1]
        self.current_sub_group_index = current_sub_group_index
        
        self.SeasonID = boxscore_data['SeasonID']
        self.GameID = boxscore_data['GameID']
        self.HomeID = boxscore_data['sql_tables']['Game']['HomeID']
        self.AwayID = boxscore_data['sql_tables']['Game']['AwayID']
        self.GameStatus = boxscore_data['sql_tables']['GameExt']['Status']
        self.home_stats = home_stats if home_stats else {}
        self.away_stats = away_stats if away_stats else {}
        self.stint_status = stint_status

        self.logger = logging.getLogger(f'StintProcessor.{self.GameID}')
        self.stint_errors = []


    def process(self):
        '''process
    ===
    Drives StintProcessor
    
    Steps
    



    
    :return transformed_playbyplay: List of Transformed PlayByPlay actions to be inserted to SQL
    :rtype: list
        '''
        self.logger.info(f'Processing Stints for {self.GameID}')

        self.team_stints = []
        self.player_stints = []
        self.tp_stints = []


        if len(self.sub_groups) == 0:
            self.current_sub_group = {'PointInGame': 99, 'NextActionNumber': 9999, 'SubTime': "", 'Period': 1, 'Clock': "0"}
            self.home, self.away = self._get_starting_lineups()
            self.home_stats, self.away_stats = self._create_initial_stats_dict(self.home, self.away, 1, 1)
        else:
            self.current_sub_group = self.sub_groups[self.current_sub_group_index]
            
        if self.db_actions == 0 or len(self.sub_groups) == 1 or self.db_actions == len(self.playbyplay_data) or self.stint_status == 'failure':
            log_str = f'Starting at first action...Getting starting lineups and creating dictionaries...'
            if self.db_actions == len(self.playbyplay_data):
                log_str = f"PlayByPlay already inserted, row count in db matches extracted data's row count...{log_str}"
            if self.stint_status == 'failure':
                log_str = f'StintStatus value for game is failure...{log_str}'
            self.logger.info(log_str)
            self.home, self.away = self._get_starting_lineups()
            self.home_stats, self.away_stats = self._create_initial_stats_dict(self.home, self.away, 1, 1)
            self.db_actions = 0
            matched_last_index = 0
        else:
            self.home = list(self.home_stats['Lineup'].keys())
            self.away = list(self.away_stats['Lineup'].keys())
            matched_last_action = next({'index': i, 'action': action} for i, action in enumerate(self.playbyplay_data) if action['actionNumber'] == self.db_last_action_number)
            matched_last_index = matched_last_action['index'] if matched_last_action.get('index') else 0

            current_action = self.playbyplay_data[matched_last_index]
            action_before = self.playbyplay_data[matched_last_index-1]
            for i, sub_group in enumerate(self.sub_groups):
                if sub_group['NextActionNumber'] >= current_action['actionNumber']:
                    self.current_sub_group = sub_group
                    self.current_sub_group_index = i
                    break
        

        
        self.home_copy = self.home.copy()
        self.away_copy = self.away.copy()
        if matched_last_index > 0:
            start_index = matched_last_index + 1
        else:
            start_index = 0

        self.condensed_playbyplay_data = self.playbyplay_data[start_index:]
        for i, action in enumerate(self.condensed_playbyplay_data):
            action_number = action['actionNumber']
            action_type = action['actionType'] 
            if action_number == self.current_sub_group['NextActionNumber'] or action_number == self.last_action['actionNumber']:
                action_prior = self.condensed_playbyplay_data[i-1]
                if action_number >= self.last_action['actionNumber'] - 5:
                    bp = 'here'
                self._switch_stint(action, action_prior)


            TeamID = action.get('teamId')
            if TeamID == None:
                continue
            is_home = TeamID == self.HomeID
            self.team_stats = self.home_stats if is_home else self.away_stats
            self.op_stats = self.away_stats if is_home else self.home_stats
            
            action_before = self.playbyplay_data[matched_last_index]

            last_possession = self.playbyplay_data[matched_last_index+i-1]['possession'] if i > 0 else 0

            if action_type == 'substitution':
                if action_number != self.last_action['actionNumber']:
                    self._initiate_substitution(action)
            elif action_type != 'substitution':
                self._increment_stats(action, last_possession)

            if len(self.stint_errors) >= 3:
                self.stint_status = 'failure'
                self.logger.critical('Three errors accumulated in StintProcessor! Cancelling...')
                break


        if len(self.stint_errors) == 0:
            self.stint_status = {
                'SeasonID': self.SeasonID,
                'GameID': self.GameID,
                'status': 'success'
            }
        else:
            self.stint_status = {
                'SeasonID': self.SeasonID,
                'GameID': self.GameID,
                'status': 'failure'
            }
            

        processed_stints = StintResult(
            Stint = self.team_stints, 
            StintPlayer = self.player_stints, 
            team_player_stints=self.tp_stints,
            status = self.stint_status,
            errors = getattr(self, 'stint_errors', None))
        bp = 'here'
        self.logger.info(f'Transformed {len(self.team_stints)} Team Stints and {len(self.player_stints)} Player Stints. Status: {processed_stints.status['status']}')
        return processed_stints


    #region Stint Changing
    def _switch_stint(self, action: dict, action_prior: dict):
        '''_summary_

        _extended_summary_

        :param action: Data for the action in which the Stint switch occurs
        :type action: dict
        :param action_prior: Data for the action *before* the action in which the Stint switch occurs
        :type action_prior: dict
        '''
        stat_dict_list, do_home, do_away = self._create_stat_dict_list()
        self._stint_end(action, action_prior, stat_dict_list, 0)

        if do_home:
            self.home_stats = self._create_team_stats(self.HomeID, self.home_stats['StintID'] + 1, action, action_prior, stat_dict_list[0]['new_lineup'])
            self.home = list(self.home_stats['Lineup'].keys())
        if do_away:
            self.away_stats = self._create_team_stats(self.AwayID, self.away_stats['StintID'] + 1, action, action_prior, stat_dict_list[1]['new_lineup'])
            self.away = list(self.away_stats['Lineup'].keys())
    # def _create_team_stats(self, SeasonID: int, GameID: int, TeamID: int, StintID: int, MinElapsedStart, action: dict, new_lineup: list):
            
        if action['actionNumber'] != self.last_action['actionNumber']:
            self.current_sub_group_index += 1
            self.sub_groups.append({
                        'PointInGame': 999,
                        'NextActionNumber': self.last_action['actionNumber'],
                        'SubTime': "",
                        'Period': 1,
                        'Clock': "0"
                    })
            self.current_sub_group = self.sub_groups[self.current_sub_group_index]
        else:
            action_prior = action
            stat_dict_list, do_home, do_away = self._create_stat_dict_list()
            self._stint_end(action, action_prior, stat_dict_list, 1)

    
    def _stint_end(self, action: dict, action_prior: dict, stat_dict_list: list, last: int):
        for dict in stat_dict_list:
            sub_needed = dict['sub_needed'] or (action['actionNumber'] == self.last_action['actionNumber'] and last == 1)
            if sub_needed == False: 
                continue
            stat_dict = dict['stats']
            stat_dict['MinutesPlayed'] = round(action_prior['MinElapsed'] - stat_dict['MinElapsedStart'], 2)
            
            #If it's the last action and the game is over:
            if action['actionNumber'] == self.last_action['actionNumber'] and 'Final' in self.GameStatus:
                stat_dict['QtrEnd'] = action_prior['period']
                stat_dict['ClockEnd'] = action_prior['Clock']
                stat_dict['MinElapsedEnd'] = action_prior['MinElapsed']
            #If it's the last action but the game is still going:
            elif action['actionNumber'] == self.last_action['actionNumber'] and 'Final' not in self.GameStatus and last == 1:
                stat_dict['QtrEnd'] = None
                stat_dict['ClockEnd'] = None
                stat_dict['MinElapsedEnd'] = None
            #If it's the start of a period that's not Q1, set the stint end to 0 and the period prior
            elif action_prior['Clock'] == '12:00.00' and action['period'] != 1:
                stat_dict['QtrEnd'] = action_prior['period'] - 1
                stat_dict['ClockEnd'] = '00:00.00'
                stat_dict['MinElapsedEnd'] = action_prior['MinElapsed']
            else:                                         
                stat_dict['QtrEnd'] = action_prior['period']
                stat_dict['ClockEnd'] = action_prior['Clock']
                stat_dict['MinElapsedEnd'] = action_prior['MinElapsed']

            for playerStats in stat_dict['Lineup'].values():
                playerStats['MinutesPlayed'] = stat_dict['MinutesPlayed']
                playerStats['PlusMinus'] = stat_dict['PtsScored'] - stat_dict['PtsAllowed']
                self.player_stints.append(playerStats.copy())

            team_stint = {key: value for key, value in stat_dict.items() if key != 'Lineup'}
            self.team_stints.append(team_stint)
            self.tp_stints.append(stat_dict)
    
    def _create_stat_dict_list(self):
        '''`_create_stat_dict_list`(self)
        ===
        <hr>

        Determines whether or not there's a Stint switch occuring for the home and/or away team
        
        :returns stat_dict_list:  
        :rtype stat_dict_list: dict
        
        :returns do_home: 
        :rtype do_home: bool
        
        :returns do_away: 
        :rtype do_away: bool
        '''    
        do_home = self.home != self.home_copy
        do_away = self.away != self.away_copy   
        stat_dict_list = [{
            'home_away': 'Home',
            'stats': self.home_stats,
            'sub_needed': do_home,
            'team_id': self.HomeID,
            'old_lineup': self.home,
            'new_lineup': self.home_copy
        },{
            'home_away': 'Away',
            'stats': self.away_stats,
            'sub_needed': do_away,
            'team_id': self.AwayID,
            'old_lineup': self.away,
            'new_lineup': self.away_copy
        }]
        return stat_dict_list, do_home, do_away


    #endregion Stint Changing


    #region Action Parsing
    def _initiate_substitution(self, action: dict):        
        if(action['actionNumber'] > action['CorrespondingSubActionNumber']):            
            other_PlayerID = next((act['personId'] for act in self.playbyplay_data if act['actionNumber'] == action['CorrespondingSubActionNumber']), None)            
            if action['teamId'] == self.HomeID:
                try:
                    home_copy = SubstitutePlayers(action, other_PlayerID, self.home_copy)
                except Exception as e:
                    self.stint_errors.append(StintError(action, self, f'{e}', 'home-sub'))
                    self.logger.error('Error during home team substitution!')
            if action['teamId'] == self.AwayID:
                try:
                    away_copy = SubstitutePlayers(action, other_PlayerID, self.away_copy)
                except Exception as e:
                    self.stint_errors.append(StintError(action, self, f'{e}', 'away-sub'))
                    self.logger.error('Error during away team substitution!')
        # except TypeError as e:
        #     self.logger.error('Subbing was not complete when data was pulled, no corresponding Player to sub in.')
        bp = 'here'



    def _increment_stats(self, action: dict, last_possession: int):
        try:
            action_type = action['actionType'].lower()
            player_id = action['personId']

            #Possession    
            if action.get('possession') and action.get('possession') != 0:
                self._parse_possession(action, last_possession)
            #Field Goal or Freethrow
            if action_type in ['2pt', '3pt', 'freethrow']:
                self._parse_fieldgoal(action)
                #Assist
                if action.get('assistPersonId'):
                    PlayerIDAst = action['assistPersonId']
                    self._parse_assist(action=action, PlayerIDAst=PlayerIDAst)

            #Rebound
            elif action_type == 'rebound':
                self._parse_rebound(action=action)

            #Block
            elif action_type == 'block':
                self._parse_block(action=action, PlayerID=player_id)

            #Steal
            elif action_type == 'steal':
                self._parse_steal(action=action, PlayerID=player_id)

            #Turnover
            elif action_type == 'turnover':
                self._parse_turnover(action=action, PlayerID=player_id)

            #Foul
            elif action_type == 'foul':
                self._parse_foul(action=action)


            
        except Exception as e:
            self.logger.error(e)
            raise
        
        return last_possession
        

    
    def _parse_possession(self, action: dict, last_possession: int):
        current_possession = action['possession']
        if pd.notna(current_possession) and current_possession != last_possession:
            if current_possession == self.team_stats['TeamID']:
                self.team_stats['Possessions'] += 1
            last_possession = current_possession

    def _parse_fieldgoal(self, action: dict):
        try:
            PlayerID = action['personId']
            shot_type = action['actionType']
            shot_result = action['shotResult']
            result_short = 'M' if shot_result == 'Made' else 'A'
            is_fieldgoal = action['isFieldGoal']
            made = shot_result == 'Made'
            if is_fieldgoal == 1:
                ShotType = f'FG{shot_type[0]}{result_short}'
                ShotValue = int(shot_type[0]) 
            else:
                ShotType = f'FT{result_short}'
                ShotValue = 1
            if made:
                self.team_stats[ShotType] += 1
                self.team_stats['PtsScored'] += ShotValue
                self.op_stats['PtsAllowed'] += ShotValue
                self.team_stats['Lineup'][PlayerID]['PTS'] += ShotValue
                self.team_stats['Lineup'][PlayerID][ShotType] += 1
            elif ' - blocked' in action['description']:
                self.team_stats['Lineup'][PlayerID]['BLKd'] += 1

            if ShotValue == 1:
                self.team_stats['FTA'] += 1
                self.team_stats['Lineup'][PlayerID]['FTA'] += 1
            elif ShotValue == 2:
                self.team_stats['FG2A'] += 1
                self.team_stats['Lineup'][PlayerID]['FG2A'] += 1
            elif ShotValue == 3:
                self.team_stats['FG3A'] += 1
                self.team_stats['Lineup'][PlayerID]['FG3A'] += 1

            self.team_stats['FGM'] = self.team_stats['FG2M'] + self.team_stats['FG3M']
            self.team_stats['FGA'] = self.team_stats['FG2A'] + self.team_stats['FG3A']
            self.team_stats['Lineup'][PlayerID]['FGM'] = self.team_stats['Lineup'][PlayerID]['FG2M'] + self.team_stats['Lineup'][PlayerID]['FG3M']
            self.team_stats['Lineup'][PlayerID]['FGA'] = self.team_stats['Lineup'][PlayerID]['FG2A'] + self.team_stats['Lineup'][PlayerID]['FG3A']
        except KeyError as e:
            self.stint_errors.append(StintError(action=action, stint_processor=self, error_msg=f'KeyError({e})', type='field-goal'))
            self.logger.error(f'KeyError on Field Goal!')
            return

    def _parse_assist(self, action: dict, PlayerIDAst):
        try:
            self.team_stats['Lineup'][PlayerIDAst]['AST'] += 1
        except KeyError as e:
            self.stint_errors.append(StintError(action=action, stint_processor=self, error_msg=f'KeyError({e})', type='assist'))
            self.logger.error(f'KeyError on Assist!')

    def _parse_rebound(self, action:dict):
        try:
            PlayerID = action['personId']
            self.team_stats['REB'] += 1
            reb_type = f'{action['subType'][0].upper()}REB'
            self.team_stats[reb_type] += 1
            if PlayerID not in [None, 0]:
                self.team_stats['Lineup'][PlayerID]['REB'] += 1
                self.team_stats['Lineup'][PlayerID][reb_type] += 1
        except KeyError as e:
            self.stint_errors.append(StintError(action=action, stint_processor=self, error_msg=f'KeyError({e})', type='rebound'))
            self.logger.error(f'KeyError on Rebound!')
            return


    def _parse_block(self, action: dict, PlayerID: int):
        try:
            self.team_stats['BLK'] += 1
            self.team_stats['Lineup'][PlayerID]['BLK'] += 1
            self.op_stats['BLKd'] += 1
        except KeyError as e:
            self.stint_errors.append(StintError(action=action, stint_processor=self, error_msg=f'KeyError({e})', type='block'))
            self.logger.error('KeyError on Block!')


    def _parse_steal(self, action: dict, PlayerID: int):
        try:
            self.team_stats['STL'] += 1
            self.team_stats['Lineup'][PlayerID]['STL'] += 1
        except KeyError as e:
            self.stint_errors.append(StintError(action=action, stint_processor=self, error_msg=f'KeyError({e})', type='steal'))
            self.logger.error('KeyError on Steal!')


    def _parse_turnover(self, action: dict, PlayerID: int):
        try:
            self.team_stats['TOV'] += 1
            if PlayerID != 0:
                self.team_stats['Lineup'][PlayerID]['TOV'] += 1
        except KeyError as e:
            self.stint_errors.append(StintError(action=action, stint_processor=self, error_msg=f'KeyError({e})', type='turnover'))
            self.logger.error('KeyError on Turnover!')


    def _parse_foul(self, action: dict):
        self.team_stats['F'] += 1

        PlayerID = action['personId']
        try:
            if PlayerID != 0:
                try:
                    self.team_stats['Lineup'][PlayerID]['F'] += 1
                except KeyError as e:
                    if action['subType'] != 'technical':
                        self.stint_errors.append(StintError(action=action, stint_processor=self, error_msg=f'KeyError({e})', type='foul'))
                        self.logger.error(f'KeyError on foul!')
                    else:
                        log_str = ' Probably due to a technical incurred by a Coach/Player not on court'
                        self.logger.warning(f'KeyError on foul!{log_str}')
            PlayerIDFoulDrawn = action.get('foulDrawnPersonId')
            if PlayerIDFoulDrawn:
                self.op_stats['FDrwn'] += 1
                self.op_stats['Lineup'][PlayerIDFoulDrawn]['FDrwn'] += 1
        
        except KeyError as e:
            self.stint_errors.append(StintError(action=action, stint_processor=self, error_msg=f'KeyError({e})', type='foul-drawn'))
            self.logger.error(f'KeyError on foul drawn!')



    #endregion Action Parsing






    #region pre-processing

    def _pre_process_existing_games(self) -> tuple[int, int, dict, dict]:
        '''
        Determines starting position of Stint Processor when data already exists for game. Part of pre-processing function group

        :returns current_action, current_sub_group_index, current_sub_group, start_sub_group: Four-element tuple containing:

            * **current_action** (*dict*): Action found at index of start_action

            * **current_sub_group_index** (*int*): Index of the current_sub_group (where the next sub occurs)

            * **current_sub_group** (*dict*): Details of when to perform the next substitution

            * **start_sub_group** (*dict*): We should get the sub group prior to the one we're on and start from their NextActionNumber value
        '''
        current_action = self.playbyplay_data[self.db_actions]
        current_sub_group_index = [i for i, sub in enumerate(self.sub_groups) if sub['PointInGame'] <= current_action['PointInGame']][::-1][0]
        current_sub_group = self.sub_groups[self.current_sub_group_index]
        start_sub_group = self.sub_groups[self.current_sub_group_index - 1]

        sub_group_start_action_Number = start_sub_group['NextActionNumber']


        return current_action, current_sub_group_index, current_sub_group, start_sub_group


    def _get_starting_lineups(self) -> tuple[list, list]:
        '''
        Returns the starting lineups for home and away teams. Part of pre-processing function group

        :returns home, away: Two-element tuple containing:

            * **home** (*list*): Home team starter PlayerIDs

            * **away** (*list*): Away team starter PlayerIDs
        '''
        lineups = self.boxscore_data['sql_tables']['StartingLineups']
        home = [player['PlayerID'] for player in lineups if player['TeamID'] == self.HomeID and player['Unit'] == 'Starters']
        away = [player['PlayerID'] for player in lineups if player['TeamID'] == self.AwayID and player['Unit'] == 'Starters']
        return home, away
    
    
    def _create_initial_stats_dict(self, home, away, home_stint: int, away_stint: int) -> tuple[dict, dict]:
        '''
        If game has yet to be loaded, create an emtpy Team Stats dictionary for each team. Part of pre-processing function group
        
        :returns home_stats, away_stats: Two-element tuple containing:

            * **home_stats** (*list*): Stats dictionary for Home Team, containing all Home Players' stat dictionaries

            * **away_stats** (*list*): Stats dictionary for Away Team, containing all Away Players' stat dictionaries
        '''
        team_stats = {
            'SeasonID': self.SeasonID,
            'GameID': self.GameID,
            'TeamID': 0,
            'StintID': 1,
            'QtrStart': 1,
            'QtrEnd': 0,
            'ClockStart': '12:00.00',
            'ClockEnd': None,
            'MinElapsedStart': 0, 
            'MinElapsedEnd': None,
            'MinutesPlayed': None,
            'Possessions': 0,
            'PtsScored': 0,
            'PtsAllowed': 0,
            'FG2M': 0,
            'FG2A': 0,
            'FG3M': 0,
            'FG3A': 0,
            'FGM': 0,
            'FGA': 0,
            'FTM': 0,
            'FTA': 0,
            'AST': 0,
            'OREB': 0,
            'DREB': 0,
            'REB': 0,
            'TOV': 0,
            'STL': 0,
            'BLK': 0,
            'BLKd': 0,
            'F': 0,
            'FDrwn': 0
        }
        home_stats = team_stats.copy()
        home_stats['TeamID'] = self.HomeID
        home_stats['StintID'] = home_stint
        home_stats['Lineup'] = {PlayerID: self._create_player_stats(PlayerID, self.HomeID, 1) for PlayerID in home}
        away_stats = team_stats.copy()
        away_stats['TeamID'] = self.AwayID
        away_stats['StintID'] = away_stint
        away_stats['Lineup'] = {PlayerID: self._create_player_stats(PlayerID, self.AwayID, 1) for PlayerID in away}


        return home_stats, away_stats
    #endregion pre-processing




    #region stat dictionaries

    def _create_team_stats(self, TeamID: int, StintID: int, action: dict, action_prior: dict, new_lineup: list):
    #Contents of a row in Stints table
        team_stats = {
            'SeasonID': self.SeasonID,
            'GameID': self.GameID,
            'TeamID': TeamID,
            'StintID': StintID,
            'QtrStart': action_prior['period'],
            'QtrEnd': 0,
            'ClockStart': action_prior['Clock'],
            'ClockEnd': None,
            'MinElapsedStart': action_prior['MinElapsed'],
            'MinElapsedEnd': None,
            'MinutesPlayed': 0,
            'Possessions': 0,
            'PtsScored': 0,
            'PtsAllowed': 0,
            'FG2M': 0,
            'FG2A': 0,
            'FG3M': 0,
            'FG3A': 0,
            'FGM': 0,
            'FGA': 0,
            'FTM': 0,
            'FTA': 0,
            'AST': 0,
            'OREB': 0,
            'DREB': 0,
            'REB': 0,
            'TOV': 0,
            'STL': 0,
            'BLK': 0,
            'BLKd': 0,
            'F': 0,
            'FDrwn': 0,
            'Lineup': {
                PlayerID: self._create_player_stats(PlayerID, TeamID, StintID)
                for PlayerID in new_lineup
                }
        }
        return team_stats

    def _create_player_stats(self, PlayerID, TeamID, StintID) -> dict:
        '''
        Generates Stats dictionary for each Player currently on court.
        
        :param TeamID: Unique ID of Team Player is playing for
        :param StintID: Unique ID of the Stint the player is partaking in 
        :param PlayerID: Unique ID of the Player stats are being recorded for

        :return player_stats (*dict*): Dictionary containing each player's stats for each stint
        '''
        player_stats = {
            'SeasonID': self.SeasonID,
            'GameID': self.GameID,
            'TeamID': TeamID,
            'StintID': StintID,
            'PlayerID': PlayerID,
            'MinutesPlayed': 0,
            'PlusMinus': 0,
            'PTS': 0,
            'AST': 0,
            'REB': 0,
            'FG2M': 0,
            'FG2A': 0,
            'FG3M': 0,
            'FG3A': 0,
            'FGM': 0,
            'FGA': 0,
            'FTM': 0,
            'FTA': 0,
            'OREB': 0,
            'DREB': 0,
            'TOV': 0,
            'STL': 0,
            'BLK': 0,
            'BLKd': 0,
            'F': 0,
            'FDrwn': 0
        }
        return player_stats

    #endregion stat dictionaries



#endregion StintProcessor


def SubstitutePlayers(action: dict, otherPlayerID: int | None, lineup: list):
    SubType = action['subType']
    PlayerID = action['personId']
    if otherPlayerID == None:
        bp = 'here'
    if SubType == 'in':
        lineup_copy = lineup.copy()
        index_out = lineup.index(otherPlayerID)
        lineup[index_out] = PlayerID
        test = lineup
        bp = 'here'
    elif SubType == 'out':
        lineup_copy = lineup.copy()
        index_out = lineup.index(PlayerID)
        lineup[index_out] = otherPlayerID
        test = lineup
        bp = 'here'


    return lineup