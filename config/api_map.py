from typing import TypedDict


stats_headers = {
    'accept': "*/*",
    'accept-encoding': "gzip, deflate, br, zstd",
    'accept-language': "en-US,en;q=0.9",
    'cache-control': "no-cache",
    'connection': 'keep-alive',
    "pragma": "no-cache",
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'sec-ch-ua-mobile': '?0',
    'host': 'stats.nba.com',
    'origin': 'https://www.nba.com',
    'referer': 'https://www.nba.com/',
    'sec-ch-ua-platform': 'Windows',
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'x-nba-stats-origin': 'stats',
    'x-nba-stats-token': 'true'
}

common_box_params = {    
    'StartPeriod': 1,
    'EndPeriod': 10,
    'StartRange': 1200,
    'EndRange': 24000,
    'RangeType': 0,
    'Season': '2025-26',
    'SeasonType': 'Regular Season',
    'GameID': 0
}




class Endpoint(TypedDict):
    url: str
    headers: dict
    params: dict

nba_advanced_stats_endpoints: dict[str, Endpoint] = {
    'leaguedashplayerstats':{
        'url': 'https://stats.nba.com/stats/leaguedashplayerstats',
        'headers': stats_headers,
        'params': {
            'ActiveRoster':     0,                  # 0, 1?
            'College':          '',                 # 
            'Conference':       '',                 # 
            'Country':          '',                 # 
            'DateFrom':         '03/05/2026',                 # 03/05/2026
            'DateTo':           '03/05/2026',                 # 03/05/2026
            'Division':         '',                 # 
            'DraftPick':        '',                 # 
            'DraftYear':        '',                 # 
            'GameScope':        '',                 # 
            'GameSegment':      '',                 # 'First Half', 'Second Half', 'Overtime'
            'Height':           '',                 # 
            'ISTRound':         '',                 # 
            'LastNGames':       0,                  # 0-15
            'LeagueID':         '00',               # 
            'Location':         '',                 # 
            'MeasureType':      'Base',             # 'Base' = Traditional, 'Advanced', 'Misc', 'Scoring', 'Usage', 'Defense', 'Violations'
            'Month':            0,                  # 1-12 (Starting October), 0 = null
            'OpponentTeamID':   0,                  # 
            'Outcome':          '',                 # 
            'PORound':          0,                  # 
            'PaceAdjust':       'N',                # 
            'PerMode':          'PerGame',          # 'Pergame', 'Totals', 'Per100Possessions', 'Per100Plays', 'Per48', 'Per40', 'Per36' 
            'Period':           0,                  # 0-14
            'PlayerExperience': '',                 # 
            'PlayerPosition':   '',                 # 'F', 'C', 'G'
            'PlusMinus':        'N',                # 
            'Rank':             'N',                # 
            'Season':           '2025-26',          # '2025-26', '2024-25', cont..., 1996-97
            'SeasonSegment':    '',                 # 'Pre All-Star', 'Post All-Star'
            'SeasonType':       'Regular Season',   # 'Pre Season', 'Regular Season', 'Playoffs', 'PlayIn', 'IST', 'All Star'
            'ShotClockRange':   '',                 # '24-22', '22-18 Very Early', '18-15 Early', '15-7 Average', '7-4 Late', '4-0 Very Late'
            'StarterBench':     '',                 # 'Starters', 'Bench'
            'TeamID':           0,                  # 
            'TwoWay':           0,                  # ?
            'VsConference':     '',                 # 
            'VsDivision':       '',                 # 
            'Weight':           '',                 # 
        }
    },
    'leaguedashptstats': {
        'url': 'https://stats.nba.com/stats/leaguedashptstats',
        'headers': stats_headers,
        'params': {
            'College':          '',                 # 
            'Conference':       '',                 # 
            'Country':          '',                 # 
            'DateFrom':         '',                 # 03/05/2026
            'DateTo':           '',                 # 03/05/2026
            'Division':         '',                 # 
            'DraftPick':        '',                 # 
            'DraftYear':        '',                 # 
            'GameScope':        '',                 # 
            'Height':           '',                 # 
            'ISTRound':         '',                 # 
            'LastNGames':       0,                  # 0-15
            'LeagueID':         '00',               # 
            'Location':         '',                 # 
            'Month':            0,                  # 1-12 (Starting October), 0 = null
            'Outcome':          '',                 # 
            'PORound':          0,                  # 
            'PerMode':          'PerGame',          # 'Pergame', 'Totals', 'Per100Possessions', 'Per100Plays', 'Per48', 'Per40', 'Per36' 
            'PlayerExperience': '',                 # 
''''''      'PlayerOrTeam':     'Player',           # 'Player', 'Team'
            'PlayerPosition':   '',                 # 'F', 'C', 'G'
''''''      'PtMeasureType':    'Drives',           # 'Drives', 'Defense', 'CatchShoot', 'Passing', 'Possessions', 'PullUpShot', 
                                                    # 'Rebounding', 'Efficiency', 'SpeedDistance', 'ElbowTouch', 'PostTouch', 'PaintTouch'
            'Season':           '2025-26',          # '2025-26', '2024-25', cont..., 1996-97
            'SeasonSegment':    '',                 # 'Pre All-Star', 'Post All-Star'
            'SeasonType':       'Regular Season',   # 'Pre Season', 'Regular Season', 'Playoffs', 'PlayIn', 'IST', 'All Star'
            'StarterBench':     '',                 # 'Starters', 'Bench'            
            'TeamID':           0,                  # 
            'VsConference':     '',                 # 
            'VsDivision':       '',                 # 
            'Weight':           '',                 # 
            
        }
    }
}

nba_advanced_stats_endpoints['leaguedashplayerstats']['params']
nba_stats_endpoints: dict[str, Endpoint] = {
############################
#region Play-By-Play
#####################
    'playbyplayv2': {
        'url': 'https://stats.nba.com/stats/playbyplayv2',
        'headers': stats_headers,
        'params': {
            'StartPeriod': 1,
            'EndPeriod': 1,
            'GameID': None,
        }
    },
    'playbyplayv3': {
        'url': 'https://stats.nba.com/stats/playbyplayv3',
        'headers': stats_headers,
        'params': {
            'StartPeriod': 1,
            'EndPeriod': 10,
            'GameID': None,
        }
    },
#endregion Play-ByPlay

############################
#region Box Score
#####################
    'boxscoretraditionalv2':{
        'url': 'https://stats.nba.com/stats/boxscoretraditionalv2',
        'headers': stats_headers,
        'params': common_box_params
    },
    'boxscoreadvancedv3':{
        'url': 'https://stats.nba.com/stats/boxscoreadvancedv3',
        'headers': stats_headers,
        'params': common_box_params
    },
    'boxscoremiscv3':{
        'url': 'https://stats.nba.com/stats/boxscoremiscv3',
        'headers': stats_headers,
        'params': common_box_params
    },
    'boxscorehustlev2':{
        'url': 'https://stats.nba.com/stats/boxscorehustlev2',
        'headers': stats_headers,
        'params': {
            'GameID': None
        }
    },
    'boxscoreplayertrackv3':{
        'url': 'https://stats.nba.com/stats/boxscoreplayertrackv3',
        'headers': stats_headers,
        'params': {
            'GameID': None
        }
    },
#endregion Box Score



############################
#region Standings
#####################
    'leaguestandingsv3':{
        'url': 'https://stats.nba.com/stats/leaguestandingsv3',
        'headers': stats_headers,
        'params': {
            'SeasonType': 'Regular Season' #'Regular Season' or 'Pre Season', potentially Playoffs...not sure if that works
        }
    },
#endregion Standings


############################
#region Teams
#####################
    'teamindex':{
        'url': 'https://stats.nba.com/stats/teamindex',
        'headers': stats_headers,
        'params': {
            'LeagueID': '00', #00 = NBA, 10 = WNBA
            'Season': 2025
        }
    },
#endregion Teams


############################
#region Players
#####################
    'playerindex':{
        'url': 'https://stats.nba.com/stats/playerindex',
        'headers': stats_headers,
        'params': {
            'LeagueID': '00', #00 = NBA, 10 = WNBA
            'Season': 2025,
            'Historical': 1, #0 or 1
        }
    },
#endregion Players


}