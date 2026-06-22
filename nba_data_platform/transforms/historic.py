import logging
from datetime import datetime
import json

class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass


    def landing(self, html_extract):
        jbox = self.box_to_json(html_extract=html_extract)
        jpbp = self.pbp_to_json(html_extract=html_extract)
        bp = 'here'


    def box_to_json(self, html_extract):
        start_box = html_extract.index('"game":')
        end_box = html_extract.index('"lastFiveMeetings"')
        box = html_extract[start_box+7:end_box-1] + '}'
        jbox = json.loads(box)
        return jbox


    def pbp_to_json(self, html_extract):
        start_pbp = html_extract.index('"playByPlay"')
        end_pbp = html_extract.index(',"source":"hanaV3"')
        pbp = html_extract[start_pbp+13:end_pbp] + '}'
        jpbp = json.loads(pbp)
        return jpbp