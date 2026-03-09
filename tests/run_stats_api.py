import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors import APIDataConnector


api = APIDataConnector('player-stats')

test = api.fetch(api.player_stats)
bp = 'here'
