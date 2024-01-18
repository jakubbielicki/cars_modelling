import os
import logging
import pandas as pd
from numpy import NaN
from pathlib import Path
from sklearn.preprocessing import LabelEncoder
from .DataProcessor import DataProcessor

class CSVDataAdjustor(DataProcessor):
    def __init__(self, params):
        super().__init__(params)
        self._input_path = self.root_path / Path("4_Cleansed_data")
        self._target_path = self.root_path / Path("5_Adjusted_data")
        self.input_file_path = self._input_path / Path("cleansed.csv")
        self.traget_file_path = self._target_path / Path("adjusted.csv")
        self._step = "Adjust DATA"

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

    def adjust_data(self):

        df = pd.read_csv(self.input_file_path, sep=';', true_values=['Tak'])

        label_encoder = LabelEncoder()

        #change formats to binary
        binary_columns = [
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
            'sport_front_seats'
            ]
  
        for binary_column in binary_columns:
            df[binary_column] = df[binary_column].map({1.0: True, NaN: False})

        #version column
        df['version'] = label_encoder.fit_transform(df['version'])

        #production_year column
        df['production_year'] = label_encoder.fit_transform(df['production_year'])

        #power column
        df['power'] = label_encoder.fit_transform(df['power'])

        #gearbox column
        df['is_gearbox_automatic'] = df['gearbox'].map({'Automatyczna': True, 'Manualna': False})

        #color column
        df = pd.get_dummies(df, columns=['color'], prefix=['color'])

        #is_car_new column
        df['is_car_new'] = df['condition'].map({'Nowe': True, 'Uzywane': False})

        #ASO_serviced column
        df.loc[df['ASO_serviced'].isnull(), 'ASO_serviced'] = False

        #wheels columns
        def categorize_wheels(row):
            if row['aluminum_wheels_16'] == True:
                return 1
            elif row['aluminum_wheels_17'] == True:
                return 2
            elif row['aluminum_wheels_18'] == True:
                return 3
            elif row['aluminum_wheels_19'] == True:
                return 4
            else:
                return 0
        df['wheels'] = df.apply(categorize_wheels, axis=1) 

        #distance_control columns
        keyless_conditions = (
            (df['front_parking_distance_control'] == True)
            | (df['rear_parking_distance_control'] == True)
        )
        df.loc[keyless_conditions, 'distance_control'] = 1    
        df['distance_control'] = df['distance_control'].map({1.0: True, NaN: False})

        #keyless_system columns
        keyless_conditions = (
            (df['keyless_entry'] == True)
            | (df['keyless_go'] == True)
            | (df['keyless_engine_start'] == True)
        )
        df.loc[keyless_conditions, 'keyless_system'] = 1
        df['keyless_system'] = df['keyless_system'].map({1.0: True, NaN: False})

        #save ready df to csv
        if not os.path.exists(self.target_path):
            os.makedirs(self.target_path)

        columns_ready = [
            'price',
            'mileage',
            'wheels',
            'power',
            'version',
            'is_gearbox_automatic',
            'ASO_serviced',
            'is_car_new',
            'heated_passanger_seat',
            'voice_control_system',
            'heated_side_mirrors',
            'hands_free_kit ',
            'leather_steering_wheel',
            'leather_upholstery',
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
            'wireless_device_charging',
            'sport_front_seats',
            'distance_control',
            'keyless_system',
            'color_Bezowy',
            'color_Biay',
            'color_Czarny',
            'color_Czerwony',
            'color_Inny_kolor',
            'color_Niebieski',
            'color_Pomaranczowy',
            'color_Srebrny',
            'color_Szary'
        ]

        df[columns_ready].to_csv(self.traget_file_path, sep=';', index=False)

        logging.info(f"Step {self._step}: data adjusted.")