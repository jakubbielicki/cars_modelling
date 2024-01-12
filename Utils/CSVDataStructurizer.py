import logging
import csv
import unicodedata
from .DataProcessor import DataProcessor
from pathlib import Path

class CSVDataStructurizer(DataProcessor):
    def __init__(self, params):
        super().__init__(params)
        self.input_path = self.root_path / Path("2_Webscrapped_data")
        self.target_path = self.root_path / Path("3_Scructured_data")
        self.column_mappings = params.get('column_mappings', {})

    def structure_data(self):

        output_file_path = self.target_path / Path("structured.csv")
        files_contents = self.process_files()
        headers = self.__get_headers()
        with open(output_file_path, mode='w', newline='', encoding='utf-8') as file:
            csv_writer = csv.DictWriter(file, delimiter=';', fieldnames=headers)
            csv_writer.writeheader()

            for file_content in files_contents.values():
                count = 0
                for row in file_content:
                    csv_writer.writerow(row)
                    count += 1
                    print(count)

    def iterate_items(self, items: list, file_name: str):

        cars_to_save = []
        for item in items:
            structured_record = self.__structure_record(item, file_name)
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