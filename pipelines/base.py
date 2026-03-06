from abc import ABC, abstractmethod
from datetime import datetime
import logging
import colorlog
from typing import TypeVar, Generic
import polars as pl
from connectors.sql import SQLConnector 


T = TypeVar('T', list, dict)

class MillisecondFormatter(colorlog.ColoredFormatter):
    def formatTime(self, time_record, datefmt = None):
        time = datetime.fromtimestamp(time_record.created)
        if datefmt:
            new_time = time.strftime(datefmt)[:-3]
            return new_time
        return time.isoformat()

class Pipeline(ABC, Generic[T]):
    '''Pipeline
===

    ### ```def __init__```(self, pipeline_name: str, pipeline_tag: str, source_tag: str):
     - Sets pipeline_name, pipeline_tag, source_tag from subclass.
     - Subclasses inherit logger, destination and run_timestamp
    
    '''

    def __init__(self, pipeline_name: str, pipeline_tag: str, source_tag: str):
        self.pipeline_name = pipeline_name
        self.pipeline_tag = pipeline_tag
        self.source_tag = source_tag
        self.logger = get_logger(pipeline_name)
        self.destination = SQLConnector(self.pipeline_name, 'JJsNBA')
        self.run_timestamp = None

    @abstractmethod
    def extract(self) -> T:
        pass
    
    @abstractmethod
    def transform(self, data: T) -> T:
        pass

    @abstractmethod
    def load(self, data: T) -> T:
        pass


    def run(self) -> dict:
        self.run_timestamp = datetime.now()
        self.logger.info('Spinning up...')

        #Extract
        self.logger.info(f'Extracting {self.pipeline_tag} via {self.source_tag}')
        data_extract = self.extract()

        #Transform
        self.logger.info(f'Transforming...')
        data_transformed = self.transform(data_extract)

        #Load
        self.logger.info(f'Loading...')
        data = self.load(data_transformed)

        return {
            'pipeline': self.pipeline_name,
            'status': 'success',
            'extracted': len(data_extract),
            'transformed': data_transformed,
            'loaded': data,
            'timestamp': self.run_timestamp.isoformat()
        }
    


def get_logger(pipeline_name):
    logger = logging.getLogger(pipeline_name)
    if not logging.root.handlers:
        handler = colorlog.StreamHandler()
        handler.setFormatter(MillisecondFormatter(
            fmt='%(log_color)s%(asctime)s:  %(name)s ╍ %(levelname)s ╍ %(message)s',
            datefmt='%m/%d/%Y %H:%M:%S.%f',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'white',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red'
            }
        ))
        logger.root.setLevel(logging.INFO)
        logger.root.addHandler(handler)
    return logger