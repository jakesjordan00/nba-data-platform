import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors import APIDataConnector, SQLConnector, StaticDataConnector
from pipelines import PlayByPlayLite


pbp = PlayByPlayLite(f'pbp.0042500202', 42500202)

pbp.run()
bp = 'here'