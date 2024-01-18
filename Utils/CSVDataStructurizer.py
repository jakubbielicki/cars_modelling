import logging
import csv
import unicodedata
from .DataProcessor import DataProcessor
from pathlib import Path

class CSVDataStructurizer(DataProcessor):
    def __init__(self, params):
        super().__init__(params)
        self._input_path = self.root_path / Path("2_Webscrapped_data")
        self._target_path = self.root_path / Path("3_Scructured_data")
        self._traget_file_path = self._target_path / Path("structured.csv")
        self._step = 'STRUCTURE DATA'
        self.column_mappings = params.get('column_mappings', {})

    @property
    def input_path(self):
        return self._input_path
    
    @property
    def target_path(self):
        return self._target_path
    
    @property
    def step(self):
        return self._step

    def structure_data(self):

        headers = self.__get_headers()
        with open(self._traget_file_path, mode='w', newline='', encoding='utf-8') as file:
            csv_writer = csv.DictWriter(file, delimiter=';', fieldnames=headers)
            csv_writer.writeheader()

            for _, cars_list in self.process_files():
                for car_row in cars_list:
                    csv_writer.writerow(car_row)

    def _iterate_items(self, items: list, file_name: str):

        cars_to_save = []
        for car_info in items:
            structured_record = self.__structure_record(car_info, file_name)
            cars_to_save.append(structured_record)

        return cars_to_save

    def __structure_record(self, car_record: dict, file_name: str) -> list:

        structured_record = car_record.copy()

        details = structured_record.pop('details')
        for key, val in details.items():
            structured_record[key] = val

        equipment = structured_record.pop('equipment')
        for equipment_item in equipment:
            structured_record[equipment_item] = "1"

        time_downloaded = file_name.replace('.json','')
        structured_record['time_downloaded'] = time_downloaded

        structured_record_norm = {}
        for key, val in structured_record.items():
            if key:
                key_norm = self.__convert_to_ascii(key)
                new_key_norm = self.column_mappings.get(key_norm, key_norm)
            if val:
                val_norm = self.__convert_to_ascii(val)

            structured_record_norm[new_key_norm] = val_norm

        return structured_record_norm

    def __convert_to_ascii(self, text: str):

        normalized_text = unicodedata.normalize('NFKD', text)
        return normalized_text.encode('ascii', 'ignore').decode('ascii')

    def __get_headers(self) -> list:

        headers = []
        for element in self.column_mappings.values():
            if element not in headers:
                headers.append(element)

        headers.append('time_downloaded')

        return headers