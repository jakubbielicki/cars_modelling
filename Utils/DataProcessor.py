import json
import os
import logging
from abc import ABC, abstractmethod
from pathlib import Path

class DataProcessor(ABC):
    def __init__(self, params):

        logging_level_config = params.get('logging_level', 'WARNING')
        logging_level = self.__get_logging_level(logging_level_config)
        logging.basicConfig(level=logging_level)

        root_path = params.get('root_path', 'C:\Cars_processing')
        self.root_path = Path(root_path)

    @property
    @abstractmethod
    def input_path(self):
        pass

    @property
    @abstractmethod
    def target_path(self):
        pass

    @property
    @abstractmethod
    def step(self):
        pass

    def process_files(self) -> (str, list):

        self.__make_dirs()
        input_files_in_dir = os.listdir(self.input_path)
        output_files_in_dir = os.listdir(self.target_path)
        counter = 0
        for file_name in input_files_in_dir:
            if file_name not in output_files_in_dir:
                input_file_path = self.input_path / Path(file_name)
                input_file = open(input_file_path, 'r')
                html_list = json.load(input_file)

                processed_list = self._iterate_items(html_list, file_name)
                yield file_name, processed_list

                counter += 1

        if counter:
            logging.info(f"Step {self.step}: All files processed.")
        else:
            logging.info(f"Step {self.step}: There were no files to process.")


    @abstractmethod
    def _iterate_items(self, items: list, file_name: str):
        pass

    def __make_dirs(self):
        self.input_path.mkdir(exist_ok=True)
        self.target_path.mkdir(exist_ok=True)

    def __get_logging_level(self, key):

        logging_levels = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }

        return logging_levels.get(key, 'WARNING')
        
