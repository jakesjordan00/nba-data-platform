import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors import APIDataConnector, SQLConnector, StaticDataConnector


api = APIDataConnector('player-stats')
sql = SQLConnector('test', 'JJsNBA')
schedule = StaticDataConnector('')



test = sql.query_to_dataframe('''
select *
from Schedule s
where s.SeasonID = 2025 and s.GameType != 'PRE' and s.GameTimeEST <= getdate()
''')

data_extract = api.fetch(api.player_stats.params)


data_extract = data_extract['resultSets'][0]
bp = 'here'

