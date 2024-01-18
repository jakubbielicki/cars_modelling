import os
import logging
import pandas as pd
from pathlib import Path
from sklearn.impute import SimpleImputer
from .DataProcessor import DataProcessor

class CSVDataCleanser(DataProcessor):
    def __init__(self, params):
        super().__init__(params)
        self._input_path = self.root_path / Path("3_Scructured_data")
        self._target_path = self.root_path / Path("4_Cleansed_data")
        self.input_file_path = self._input_path / Path("structured.csv")
        self.traget_file_path = self._target_path / Path("cleansed.csv")
        self._step = "Cleanse DATA"

    @property
    def input_path(self):
        return self._input_path
    
    @property
    def target_path(self):
        return self._target_path
    
    @property
    def step(self):
        return self._step
    
    def _iterate_items(self, items: list):
        pass

    def cleanse_data(self):

        df_loaded = pd.read_csv(self.input_file_path, sep=';')

        columns = [
            'price',
            'seller',
            'version',
            'production_year',
            'mileage',
            'fuel_type',
            'power',
            'gearbox',
            'color',
            'ASO_serviced',
            'condition',
            'damaged',
            'heated_passanger_seat',
            'voice_control_system',
            'heated_side_mirrors',
            'hands_free_kit ',
            'steel_wheels',
            'aluminum_wheels_16',
            'aluminum_wheels_17',
            'aluminum_wheels_18',
            'aluminum_wheels_19',
            'leather_steering_wheel',
            'leather_upholstery',
            'front_parking_distance_control',
            'rear_parking_distance_control',
            'rear_parking_camera',
            'park_assistant',
            'apple_carplay',
            'heated_driver_seat',
            'leather_gear_shift',
            'adaptive_cruise_control_acc',
            'automatic_air_conditioning',
            'rain_sensor',
            'led_headlights',
            'fog_lights',
            'led_tail_lights',
            'led_interior_lighting',
            'cornering_lights',
            'home_pathway_lighting',
            'blind_spot_sensor',
            'keyless_entry',
            'keyless_go',
            'keyless_engine_start',
            'wireless_device_charging',
            'sport_front_seats',
            'time_downloaded'
        ]

        #deduplicate data
        distinct_columns = columns.copy()
        for col_to_remove in ['price', 'time_downloaded']:
            distinct_columns.remove(col_to_remove)

        df = df_loaded[columns].drop_duplicates(subset=distinct_columns)
        del df_loaded

        #price column
        df['price'] = df['price'].str.replace(' ', '')
        df['price'] = df['price'].astype(int)
        df = df[df['price']<=180000]

        #version column
        df.loc[df['version'] == '1.0 TSI', 'version'] = '1.0 TSI Life'
        version_filter = ['1.0 TSI Active', 
                     '1.0 TSI Active DSG', 
                     '1.0 TSI OPF ACTIVE', 
                     '1.0 TSI OPF DSG Style', 
                     '1.0 TSI OPF DSG UNITED',
                     '1.0 TSI United',
                     '1.0 TSI United DSG',
                     '1.5 TSI ACT United DSG',
                     '1.6 TDI SCR DSG',
                     '1.6 TDI SCR Style']
        df.loc[df['version'].isin(version_filter), 'version'] = 'other'

        imputer = SimpleImputer(strategy='most_frequent')
        df['version'] = imputer.fit_transform(df[['version']]).ravel()

        #production_year column
        df = df[df['production_year']>=2018]

        #fuel_type column
        df = df[df['fuel_type']=='Benzyna']

        #power column
        df.loc[df['power']=='116 KM', 'power'] = '115 KM'

        #color column
        filter_color = ['Bekitny',
                        'Brazowy',
                        'Zielony',
                        'Zoty']
        df.loc[df['color'].isin(filter_color), 'color'] = 'Inny_kolor'

        #damaged column
        df = df[df['damaged'].isnull()]

        #mileage column
        df['mileage'] = df['mileage'].str.replace(' km', '').str.replace(' ', '')
        df.loc[(df['mileage'].isnull()) & (df['condition']=='Nowe'), 'mileage'] = 0

        #save ready df to csv
        if not os.path.exists(self.target_path):
            os.makedirs(self.target_path)

        df.to_csv(self.traget_file_path, sep=';', index=False)

        logging.info(f"Step {self._step}: data cleansed.")