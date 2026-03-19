import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import pyperclip
from connectors.sql import SQLConnector
connector = SQLConnector('generate_unit_test_data', 'JJsNBA')

print("cwd:", os.getcwd())
print("'tests' exists:", os.path.exists('.tests'))
print("is dir:", os.path.isdir('.tests'))


def run():
    try:
        print('Reading clipboard first...')
        #Use line below normally
        game_id = pyperclip.paste().replace(', ', ' ',).replace('\r', '').replace(',', ' ').replace('\n', ' ').strip()

        # game_id = '22500792 22500793 22500794 22500795 22500796 22500797 22500798 22500799 22500800 22500801' #replace this
        # game_id = '22500799 22500798' #replace this
        #game_id = '22500798' #replace this

        games = game_id.split(' ')
        if games[0].lstrip('0')[1:3] == '':
            games[0] = 'aaaaaaaaaa'
        season_id = 2000 + int(f'{games[0].lstrip('0')[1:3]}')
    except Exception as e:
        print('Clipboard text not valid!')
        try:
            game_id = input('enter GameID(s): ').replace(', ', ' ',).replace(',', ' ').strip()
            games = game_id.split(' ')
            season_id = 2000 + int(f'{games[0][1:3]}')
        except Exception as e:
            print('Text not valid!')
            bp = 'here'



    if games:
        try:
            game_str = ', '.join(game.lstrip('0') for game in games if game != '')
            print('Success!')
            print(f'SeasonID: {season_id}')
            print(f'Games: {game_str}')
        except Exception as e:
            print(f'Could not parse SeasonID from GameID input.\n{e}')
            bp = 'here'
    else: 
        return
    
    keys = {
        'season_id': str(season_id),
        'game_id': game_str
    }
    Schedule = connector.cursor_query('Schedule', keys)
    return Schedule['schedule']






def format(schedule):
    game_strings = []
    for game in schedule:
        game_string = f'''
            {{
                "gameId": "00{game['GameID']}",
                "gameCode": "",
                "gameStatus": 2,
                "gameStatusText": "{game['Status']}",
                "period": 4,
				"gameClock": "",
				"gameTimeUTC": "2026-02-21T00:00:00Z",
				"gameEt": "2026-02-20T19:00:00Z",
				"regulationPeriods": 4,
				"ifNecessary": false,
				"seriesGameNumber": "",
				"gameLabel": "",
				"gameSubLabel": "",
				"seriesText": "",
				"seriesConference": "",
				"poRoundDesc": "",
				"gameSubtype": "",
				"isNeutral": false,
				"homeTeam": {{
					"teamId": {game['HomeID']},
					"teamName": "{game['HomeName']}",
					"teamCity": "{game['HomeCity']}",
					"teamTricode": "{game['HomeTri']}",
					"wins": {game['HomeWins']},
					"losses": {game['HomeLosses']},
					"score": {game['HomeScore']},
					"seed": {game['HomeSeed'] if game['HomeSeed'] != None else 'null'},
					"inBonus": null,
					"timeoutsRemaining": 0
                }},
				"awayTeam": {{
					"teamId": {game['AwayID']},
					"teamName": "{game['AwayName']}",
					"teamCity": "{game['AwayCity']}",
					"teamTricode": "{game['AwayTri']}",
					"wins": {game['AwayWins']},
					"losses": {game['AwayLosses']},
					"score": {game['AwayScore']},
					"seed": {game['AwaySeed'] if game['AwaySeed'] != None else 'null'},
					"inBonus": null,
					"timeoutsRemaining": 0
                }}
            }}'''
        game_strings.append(game_string)
    bp = 1
    long_string = ','.join(gstring for gstring in game_strings)
    return long_string




schedule = run()

long_string = format(schedule)
bp = 'here'

todays_scoreboard = f'''{{
	"meta": {{
		"version": 1,
		"request": "https://nba-prod-us-east-1-mediaops-stats.s3.amazonaws.com/NBA/liveData/scoreboard/todaysScoreboard_00.json",
		"time": "2026-02-20 02:42:34.4234",
		"code": 200
	}},
    "scoreboard": {{
		"gameDate": "2026-02-20",
		"leagueId": "00",
		"leagueName": "National Basketball Association",
		"games": [{long_string}
        ]
    }}
}}'''


with open('.tests/generated_scoreboard.json', 'w') as f:
    f.write(todays_scoreboard)

print(todays_scoreboard)
