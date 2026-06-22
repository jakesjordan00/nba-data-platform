from nba_data_platform.pipelines import Pipeline
from nba_data_platform.connectors import SeleniumWebDriver
from nba_data_platform.transforms.historic import Transform

class HistoricData(Pipeline):
    def __init__(self):
        super().__init__(pipeline_name='historic-data', pipeline_tag='historic', source_tag='selenium')
        self.source = self.destination
        self.transformer = Transform(self)
        self.swd = SeleniumWebDriver(self)
        self.games = [g['GameID'] for g in self.source.query_db('select distinct GameID from Game g where SeasonID = 2000').to_dicts()]




    def extract(self, game: int):
        url = f'https://www.nba.com/game/atl-vs-phi-00{game}'
        self.logger.info(f'{game}: Going to nba.com page ({url})')
        self.swd.go_to(url = url)
        html_extract = self.swd.driver.page_source
        return html_extract
    #

    def transform(self, html_extract):
        data_transformed = self.transformer.landing(html_extract=html_extract)
        return data_transformed
    

    def load(self, data_transformed):
        data_loaded = data_transformed
        return data_loaded
    

    def run(self):
        for game in self.games:
            data_extract = self.extract(game=game)

            data_transformed = self.transform(data_extract)

            data_loaded = data_transformed
            bp = 'here'