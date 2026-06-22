import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nba_data_platform.pipelines import DownloadVideos





download = DownloadVideos()
download.run()