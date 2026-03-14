from sql import query, table

import os
from dotenv import load_dotenv
load_dotenv() 


DATABASES = {
    'JJsNBA':{
        'server': os.getenv('ServerIP'),
        'database': 'JJsNBA', #os.getenv('Database'),
        'username': 'jjAdmin',
        'password': os.getenv('AdminPass')
    }
}

TABLES = {
    'Team':{
        'keys': ['SeasonID', 'TeamID',],
        'columns': ['SeasonID',
            'TeamID',
            'City',
            'Name',
            'Tricode',
            'Wins',
            'Losses',
            'FullName',
            'Conference',
            'Division'
        ],
        'update_columns': ['Wins', 'Losses'],
        'create': table(name='Team')
    },
    'Arena':{
        'keys': ['SeasonID', 'ArenaID'],
        'columns': [
            'SeasonID',
            'ArenaID',
            'TeamID',
            'City',
            'Country',
            'Name',
            'PostalCode',
            'State',
            'StreetAddress',
            'Timezone'
        ],
        'update_columns': [],
        'create': table(name='Arena')
    },
    'Official':{
        'keys': ['SeasonID', 'OfficialID'],
        'columns': [
            'SeasonID',
            'OfficialID',
            'Name',
            'Number'
        ],
        'update_columns': [],
        'create': table(name='Official')
    },
    'Player':{
        'keys': ['SeasonID', 'PlayerID'],
        'columns': [
            'SeasonID',
            'PlayerID',
            'Name',
            'Number',
            'Position',
            'NameInitial',
            'NameLast',
            'NameFirst'
        ],
        'update_columns': [],
        'create': table(name='Player')
    },
    'Game':{
        'keys': ['SeasonID', 'GameID'],
        'columns':[
            'SeasonID',
            'GameID',
            'Date',
            'GameType',
            'HomeID',
            'HScore',
            'AwayID',
            'AScore',
            'WinnerID',
            'WScore',
            'LoserID',
            'LScore',
            'SeriesID',
            'Datetime',
        ],
        'update_columns':[
            'HScore', 
            'AScore', 
            'WinnerID', 
            'WScore', 
            'LoserID', 
            'LScore'
        ],
        'create': table(name='Game')
    },
    'GameExt':{
        'keys': ['SeasonID', 'GameID'],
        'columns':[
            'SeasonID',
            'GameID',
            'ArenaID', 
            'Attendance', 
            'Sellout', 
            'Label', 
            'LabelDetail', 
            'OfficialID', 
            'Official2ID', 
            'Official3ID', 
            'OfficialAlternateID', 
            'Status', 
            'Periods'
        ],
        'update_columns':[
            'Attendance', 
            'Sellout', 
            'Label', 
            'LabelDetail', 
            'Status', 
            'Periods'
        ],
        'create': table(name='GameExt')
    },
    'TeamBox':{
        'keys':['SeasonID', 'GameID', 'TeamID', 'MatchupID'],
        'columns':[
            'SeasonID',
            'GameID',
            'TeamID',
            'MatchupID',
            'Points',
            'PointsAgainst',
            'FG2M',
            'FG2A',
            '[FG2%]',
            'FG3M',
            'FG3A',
            '[FG3%]',
            'FGM',
            'FGA',
            '[FG%]',
            'FieldGoalsEffectiveAdjusted',
            'FTM',
            'FTA',
            '[FT%]',
            'SecondChanceFGM',
            'SecondChanceFGA',
            '[SecondChanceFG%]',
            'TrueShootingAttempts',
            'TrueShootingPercentage',
            'PtsFromTurnovers',
            'PtsSecondChance',
            'PtsInThePaint',
            'PaintFGM',
            'PaintFGA',
            '[PaintFG%]',
            'PtsFastBreak',
            'FastBreakFGM',
            'FastBreakFGA',
            '[FastBreakFG%]',
            'BenchPoints',
            'ReboundsDefensive',
            'ReboundsOffensive',
            'ReboundsPersonal',
            'ReboundsTeam',
            'ReboundsTeamDefensive',
            'ReboundsTeamOffensive',
            'ReboundsTotal',
            'Assists',
            'AssistsTurnoverRatio',
            'BiggestLead',
            'BiggestLeadScore',
            'BiggestScoringRun',
            'BiggestScoringRunScore',
            'TimeLeading',
            'TimesTied',
            'LeadChanges',
            'Steals',
            'Turnovers',
            'TurnoversTeam',
            'TurnoversTotal',
            'Blocks',
            'BlocksReceived',
            'FoulsDrawn',
            'FoulsOffensive',
            'FoulsPersonal',
            'FoulsTeam',
            'FoulsTeamTechnical',
            'FoulsTechnical',
            'Wins',
            'Losses',
            'Win',
            'Seed'
        ],
        'update_columns': [
            'Points',
            'PointsAgainst',
            'FG2M',
            'FG2A',
            '[FG2%]',
            'FG3M',
            'FG3A',
            '[FG3%]',
            'FGM',
            'FGA',
            '[FG%]',
            'FieldGoalsEffectiveAdjusted',
            'FTM',
            'FTA',
            '[FT%]',
            'SecondChanceFGM',
            'SecondChanceFGA', 
            '[SecondChanceFG%]',
            'TrueShootingAttempts',
            'TrueShootingPercentage',
            'PtsFromTurnovers',
            'PtsSecondChance',
            'PtsInThePaint',
            'PaintFGM',
            'PaintFGA',
            '[PaintFG%]',
            'PtsFastBreak',
            'FastBreakFGM',
            'FastBreakFGA',
            '[FastBreakFG%]',
            'BenchPoints',
            'ReboundsDefensive',
            'ReboundsOffensive',
            'ReboundsPersonal',
            'ReboundsTeam',
            'ReboundsTeamDefensive',
            'ReboundsTeamOffensive',
            'ReboundsTotal',
            'Assists',
            'AssistsTurnoverRatio',
            'BiggestLead',
            'BiggestLeadScore',
            'BiggestScoringRun',
            'BiggestScoringRunScore',
            'TimeLeading',
            'TimesTied',
            'LeadChanges',
            'Steals',
            'Turnovers',
            'TurnoversTeam',
            'TurnoversTotal',
            'Blocks',
            'BlocksReceived',
            'FoulsDrawn',
            'FoulsOffensive',
            'FoulsPersonal',
            'FoulsTeam',
            'FoulsTeamTechnical',
            'FoulsTechnical',
            'Wins', 
            'Losses',
            'Win',
            'Seed'
        ],
        'create': table(name='TeamBox')
    },
    'PlayerBox':{
        'keys': ['SeasonID', 'GameID', 'TeamID', 'MatchupID', 'PlayerID'],
        'columns': [
            'SeasonID',
            'GameID',
            'TeamID',
            'MatchupID',
            'PlayerID',
            'Status',
            'Starter', 
            'Position', 
            'Minutes', 
            'MinutesCalculated', 
            'Points',
            'Assists',
            'ReboundsTotal',
            'FG2M',
            'FG2A',
            '[FG2%]',
            'FG3M', 
            'FG3A',
            '[FG3%]',
            'FGM', 
            'FGA', 
            '[FG%]', 
            'FTM', 
            'FTA', 
            '[FT%]', 
            'ReboundsDefensive', 
            'ReboundsOffensive', 
            'Blocks', 
            'BlocksReceived', 
            'Steals', 
            'Turnovers', 
            'AssistsTurnoverRatio', 
            'Plus', 
            'Minus', 
            'PlusMinusPoints',
            'PtsFastBreak',
            'PtsInThePaint',
            'PtsSecondChance', 
            'FoulsOffensive', 
            'FoulsDrawn', 
            'FoulsPersonal', 
            'FoulsTechnical', 
            'StatusReason', 
            'StatusDescription'

        ],
        'update_columns': [
            'Status',
            'Starter',
            'Position',
            'Minutes',
            'MinutesCalculated',
            'Points',
            'Assists',
            'ReboundsTotal',
            'FG2M',
            'FG2A',
            '[FG2%]',
            'FG3M',
            'FG3A',
            '[FG3%]',
            'FGM',
            'FGA',
            '[FG%]',
            'FTM',
            'FTA',
            '[FT%]',
            'ReboundsDefensive',
            'ReboundsOffensive',
            'Blocks',
            'BlocksReceived',
            'Steals',
            'Turnovers',
            'AssistsTurnoverRatio',
            'Plus',
            'Minus',
            'PlusMinusPoints',
            'PtsFastBreak',
            'PtsInThePaint',
            'PtsSecondChance',
            'FoulsOffensive',
            'FoulsDrawn',
            'FoulsPersonal',
            'FoulsTechnical',
            'StatusReason',
            'StatusDescription'
        ],
        'create': table(name='PlayerBox')
    },    
    'StartingLineups':{
        'keys': ['SeasonID', 'GameID', 'TeamID', 'MatchupID', 'PlayerID'],
        'columns': [
            'SeasonID',
            'GameID',
            'TeamID',
            'MatchupID',
            'PlayerID',
            'Unit',
            'Position'
        ],
        'update_columns': [],
        'create': table(name='StartingLineups')
    },
    'PlayByPlay':{
        'keys': [],
        'columns': [
            'SeasonID',
            'GameID',
            'ActionID',
            'ActionNumber',
            'Qtr',
            'Clock',
            'TimeActual',
            'ScoreHome',
            'ScoreAway',
            'Possession',
            'TeamID',
            'Tricode',
            'PlayerID',
            'Description',
            'SubType',
            'IsFieldGoal',
            'ShotResult',
            'ShotValue',
            'ActionType',
            'ShotDistance',
            'Xlegacy',
            'Ylegacy',
            'X',
            'Y',
            'Location',
            'Area',
            'AreaDetail',
            'Side',
            'ShotType',
            'PtsGenerated',
            'Descriptor',
            'Qual1',
            'Qual2',
            'Qual3',
            'ShotActionNbr',
            'PlayerIDAst',
            'PlayerIDBlk',
            'PlayerIDStl',
            'PlayerIDFoulDrawn',
            'PlayerIDJumpW',
            'PlayerIDJumpL',
            'OfficialID',
            'QtrType'
        ],
        'update_columns': [],
        'check_query': '''select count(p.ActionID) Actions
, max(ActionNumber) LastActionNumber
, s.Status
from PlayByPlay p
left join StintStatus s on p.SeasonID = s.SeasonID and p.GameID = s.GameID
where p.SeasonID = season_id and p.GameID = game_id
group by s.Status''',
        'create': table(name='PlayByPlay')
    },
    'StintStatus':{
        'keys': ['SeasonID', 'GameID'],
        'columns': [
            'SeasonID',
            'GameID'
        ],
        'update_columns': ['status'],
        'create': table(name='StintStatus')
    },
    'Stint':{
        'keys': ['SeasonID', 'GameID', 'TeamID', 'StintID'],
        'columns': [
            'SeasonID',
            'GameID',
            'TeamID',
            'StintID',
            'QtrStart',
            'QtrEnd',
            'ClockStart',
            'ClockEnd',
            'MinElapsedStart',
            'MinElapsedEnd',
            'MinutesPlayed',
            'Possessions',
            'PtsScored',
            'PtsAllowed',
            'FG2M',
            'FG2A',
            'FG3M',
            'FG3A',
            'FGM',
            'FGA',
            'FTM',
            'FTA',
            'OREB',
            'DREB',
            'REB',
            'AST',
            'TOV',
            'STL',
            'BLK',
            'BLKd',
            'F',
            'FDrwn'
        ],
        'update_columns': [
            'QtrEnd',
            'ClockEnd',
            'MinElapsedEnd',
            'MinutesPlayed',
            'Possessions',
            'PtsScored',
            'PtsAllowed',
            'FG2M',
            'FG2A',
            'FG3M',
            'FG3A',
            'FGM',
            'FGA',
            'FTM',
            'FTA',
            'OREB',
            'DREB',
            'REB',
            'AST',
            'TOV',
            'STL',
            'BLK',
            'BLKd',
            'F',
            'FDrwn'
        ],
        'check_query': '''
with TeamsOnCourt as(
select s.*
, dense_rank() over(partition by TeamID order by StintID desc) OnCourt
from Stint s
where s.SeasonID = season_id and s.GameID = game_id
)
select *
from TeamsOnCourt
where OnCourt = 1
order by OnCourt asc,TeamID''',
        'create': table(name='Stint')
    },
    'StintPlayer':{
        'keys': ['SeasonID', 'GameID', 'TeamID', 'StintID', 'PlayerID'],
        'columns': [
            'SeasonID',
            'GameID',
            'TeamID',
            'StintID',
            'PlayerID',
            'MinutesPlayed',
            'PlusMinus',
            'PTS',
            'AST',
            'REB',
            'FG2M',
            'FG2A',
            'FG3M',
            'FG3A',
            'FGM',
            'FGA',
            'FTM',
            'FTA',
            'OREB',
            'DREB',
            'TOV',
            'STL',
            'BLK',
            'BLKd',
            'F',
            'FDrwn'
        ],
        'update_columns': [
            'MinutesPlayed',
            'PlusMinus',
            'PTS',
            'AST',
            'REB',
            'FG2M',
            'FG2A',
            'FG3M',
            'FG3A',
            'FGM',
            'FGA',
            'FTM',
            'FTA',
            'OREB',
            'DREB',
            'TOV',
            'STL',
            'BLK',
            'BLKd',
            'F',
            'FDrwn'
        ],
        'check_query': '''
with PlayersOnCourt as(
select sp.*
, dense_rank() over(partition by TeamID order by StintID desc) OnCourt
from StintPlayer sp
where sp.SeasonID = season_id and sp.GameID = game_id
)
select *
from PlayersOnCourt
where OnCourt = 1
order by OnCourt asc,TeamID, PlayerID
''',
        'create': table(name='StintPlayer')
    },
    'Schedule': {
        'check_query': '''
select s.SeasonID
, s.GameID
, s.Status
, s.HomeID
, h.Name HomeName
, h.City HomeCity
, h.Tricode HomeTri
, s.HomeWins
, s.HomeLosses
, s.HomeScore
, s.HomeSeed
, s.AwayID
, a.Name AwayName
, a.City AwayCity
, a.Tricode AwayTri
, s.AwayWins
, s.AwayLosses
, s.AwayScore
, s.AwaySeed
, s.IfNecessary
, s.SeriesText
from Schedule s
left join Team h on s.SeasonID = h.SeasonID and s.HomeID = h.TeamID
left join Team a on s.SeasonID = a.SeasonID and s.AwayID = a.TeamID
where s.SeasonID = season_id and s.GameID in(game_id)
''',
        'create': table(name='Schedule')
    },
    'DailyLineups': {
        'keys': ['SeasonID', 'GameID', 'TeamID', 'MatchupID', 'PlayerID'],
        'columns': [
            'SeasonID', 
            'GameID', 
            'TeamID', 
            'MatchupID', 
            'PlayerID', 
            'Position',
            'LineupStatus',
            'RosterStatus',
            'Timestamp'
        ],
        'update_columns': [
            'Position',
            'LineupStatus',
            'RosterStatus',
            'Timestamp'
        ],
        'create': table('DailyLineups')
    },
    'adv.PlayerBox': {
        'keys': ['SeasonID', 'GameID', 'TeamID', 'MatchupID', 'PlayerID'],
        'columns': [
            'SeasonID','GameID', 'TeamID', 'MatchupID', 'PlayerID',
            'Minutes',
            'OffRTG',
            'DefRTG',
            'NetRTG',
            '[Ast%]',
            'ATR',
            'AstRatio',
            '[OReb%]',
            '[DReb%]',
            '[Reb%]',
            '[TeamTO%]',
            '[EFG%]',
            '[TS%]',
            '[Usage%]',
            'Pace',
            'PacePer40',
            'PIE',
            'POSS',
            'FGM',
            'FGA',
            '[FG%]',
        ],
        'update_columns': [
            'Minutes',
            'OffRTG',
            'DefRTG',
            'NetRTG',
            '[Ast%]',
            'ATR',
            'AstRatio',
            '[OReb%]',
            '[DReb%]',
            '[Reb%]',
            '[TeamTO%]',
            '[EFG%]',
            '[TS%]',
            '[Usage%]',
            'Pace',
            'PacePer40',
            'PIE',
            'POSS',
            'FGM',
            'FGA',
            '[FG%]',
        ],
        'create': table('adv.PlayerBox')
    },
#     'placeholder': {
#         'keys': [],
#         'columns': [
#         ],
#         'update_columns': [
#         ],
#         'create': '''
# if not exists(
# select 1 from sys.tables t where t.name = 'DailyLineups'
# ) 
# begin

# end
# '''    }
}