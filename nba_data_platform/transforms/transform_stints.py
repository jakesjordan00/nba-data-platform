from tracemalloc import start
import pandas as pd
import polars as pl
#region Substitution Groups



def determine_substitutions(playbyplay_data: list, boxscore_data: dict):
    '''Summary
-------------
Adds substitution information to playbyplay_data and creates sub_groups, which are used by StintProcessor. 

:param dict playbyplay_data: Extracted PlayByPlay data
:param dict boxscore_data: Transformed Boxscore data

Function Calls
-------------
*   **calculate_point_in_game(Clock, Qtr, Periods)**
    - Determines MinElapsed and PointInGame values for a given action

*   **find_next_action_number(action, i, playbyplay_data, game_end)**
    - When a subsitution occurs, finds the next action number that is *NOT* a sub.

:return playbyplay_data (list[dict]): Updated version of original PlayByPlay data extract.

:return sub_groups (list[dict]): Used in StintProcessor. Tells us when to process a substitution & switch stints

>>>     sub_groups[0] = {
            'PointInGame': 9.0211, 
            'NextActionNumber': 56, 
            'SubTime': 'Q1 06:19.00', 
            'Period': 1, 
            'Clock':'06:19.00', 
            'MinElapsed': 5.6833, 
            'Index': 41
        }

*   Items added to playbyplay_data:
    * **action['PointInGame']**: Decimal number representing the progress of the game (0.0 - 100.0)
    * **action['MinElapsed']**: How many minutes (calculated) have passed in the game. (Q1 '05:30' -> 6.5)
    * **action['Clock']**: Parsed version of clock ('PT12M00.00S' -> '12:00:00')
    * **action['SubInNumber']**: Count of substitutions *in* that have taken place thus far
    * **action['SubOutNumber']**: Count of substitutions *out* that have taken place thus far
    * **action['TeamSubInNumber']**: Count of substitutions *in* **by team** that have taken place thus far
    * **action['TeamSubOutNumber']**: Count of substitutions *out* **by team** that have taken place thus far
    * **action['Index']**: Index of action
    '''
    sub_in_actions = 0
    sub_out_actions = 0
    home_in = 0
    home_out = 0
    away_in = 0
    away_out = 0
    sub_groups = []
    
    Periods = 4 if boxscore_data['sql_tables']['GameExt']['Periods'] <= 4 else boxscore_data['sql_tables']['GameExt']['Periods']
    for i, action in enumerate(playbyplay_data):
        Qtr = action['period']
        Clock = action['clock'].replace('PT', '').replace('M', ':').replace('S', '')
        PointInGame, MinElapsed = calculate_point_in_game(Clock, Qtr, Periods)
        action['PointInGame'] = PointInGame
        action['MinElapsed'] = MinElapsed
        action['Clock'] = Clock
        action['Index'] = i

        game_end = 1 if action['actionType'] == 'game' and action['subType'] == 'end' else 0
        #This is where i need to handle if the most recent action is a substitution.
        if action['actionType'] == 'substitution' or (game_end == 1):
            SubTime = f'Q{Qtr} {Clock}'
            NextActionNumberOld = playbyplay_data[i+1]['actionNumber'] if game_end == 0 and i+1 < len(playbyplay_data) else action['actionNumber']
            next_action_number = find_next_action_number(action, i, playbyplay_data, game_end)
            test  = [s['SubTime'] for s in sub_groups]
            if (SubTime, next_action_number) not in [(s['SubTime'], s['NextActionNumber']) for s in sub_groups]:
                sub_groups.append({
                    'PointInGame': PointInGame,
                    'NextActionNumber': next_action_number,
                    'SubTime': SubTime,
                    'Period': Qtr,
                    'Clock': Clock,
                    'MinElapsed': MinElapsed,
                    'Index': i
                })
            else:
                existing = next(s for s in sub_groups if s['SubTime'] == SubTime and next_action_number == s['NextActionNumber'])
                existing['NextActionNumber'] = next_action_number

            if action['subType'] == 'in':
                sub_in_actions += 1
                action['SubInNumber'] = sub_in_actions
                action['SubOutNumber'] = None
                if action['teamId'] == boxscore_data['sql_tables']['Game']['HomeID']:
                    home_in += 1
                    team_in_actions = home_in
                elif action['teamId'] == boxscore_data['sql_tables']['Game']['AwayID']:
                    away_in += 1
                    team_in_actions = away_in
                action['TeamSubInNumber'] = team_in_actions
                action['TeamSubOutNumber'] = None

            elif action['subType'] == 'out':
                sub_out_actions += 1
                action['SubInNumber'] = None
                action['SubOutNumber'] = sub_out_actions
                if action['teamId'] == boxscore_data['sql_tables']['Game']['HomeID']:
                    home_out += 1
                    team_out_actions = home_out
                elif action['teamId'] == boxscore_data['sql_tables']['Game']['AwayID']:
                    away_out += 1
                    team_out_actions = away_out
                action['TeamSubInNumber'] = None
                action['TeamSubOutNumber'] = team_out_actions
        action['SubInNumber'] = action['SubInNumber'] if action.get('SubInNumber') else None
        action['SubOutNumber'] = action['SubOutNumber'] if action.get('SubOutNumber') else None
        action['TeamSubInNumber'] = action['TeamSubInNumber'] if action.get('TeamSubInNumber') else None
        action['TeamSubOutNumber'] = action['TeamSubOutNumber'] if action.get('TeamSubOutNumber') else None



    for action in playbyplay_data:
        if action['actionType'] == 'substitution':
            bp = 'here'
            if action['subType'] == 'in':
                subs_type = 'SubIn'
                opp_subs_type = 'SubOut'
            else:
                subs_type = 'SubOut'
                opp_subs_type = 'SubIn'
            try:
                action['CorrespondingSubActionNumber'] = next(
                    (p['actionNumber'] for p in playbyplay_data
                    if p['actionNumber'] != action['actionNumber']  # don't match itself
                    and action[f'Team{subs_type}Number'] == p[f'Team{opp_subs_type}Number']
                    and action['teamId'] == p['teamId']
                    and action['period'] == p['period']
                    and action['Clock'] == p['Clock']),
                    None  # default if no match found
                )
                bp = 'here'
            except Exception as e:
                test = playbyplay_data
                bp = 'here'
            if action['CorrespondingSubActionNumber'] == None:
                bp = 'here'


    return playbyplay_data, sub_groups



def calculate_point_in_game(Clock: str, Period: int, Periods: int):
    '''`calculate_point_in_game`(Clock: *str*, Period: *int*, Periods: *int*)
    ---
    <hr>


    Calculates the percent through the game 
    '''
    cMinutes = int(Clock[0:2])
    cSeconds = float(Clock[-5:])
    MinElapsed = round(12 - (cMinutes + (cSeconds / 60)) + ((Period - 1) * 12), 4)
    minCalc = (cMinutes + (cSeconds / 60))


    total_game_time = (12 * 4) + ((Periods - Period) * 5)
    PointInGame = round(((MinElapsed / total_game_time) * 100), 4)
    return PointInGame, MinElapsed


def find_next_action_number(action: dict, i: int, playbyplay_data: list, game_end: int):
    '''Summary
-------------
Finds the next action_number in PlayByPlay list that is NOT a substituion

When processing Stints, we'll switch our Stint when the current action_number = our respective sub group's NextActionNumber
    '''
    if game_end == 1 or i + 1 >= len(playbyplay_data):
        return action['actionNumber']
    try:
        next_action_number = [p['actionNumber'] for p in playbyplay_data[i:] if p['actionType'] != 'substitution'][0]
    except IndexError as e:
        #If there aren't any actions after this one that are substitutions
        next_action_number = playbyplay_data[-1]['actionNumber']
    return next_action_number
