import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nba_data_platform.pipelines import DailyLineupsPipeline

test = DailyLineupsPipeline('lineups')
a = test.run()
bp = 'here'