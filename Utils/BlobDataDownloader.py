import os
import logging
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from .DataProcessor import DataProcessor

class BlobDataDownloader(DataProcessor):
    def __init__(self, params):
        super().__init__(params)
        self._input_path = None
        self._target_path = self.root_path / Path(r"1_Raw_from_blob")
        self._step = "DOWNLOAD DATA"
        self.connection_string = os.environ['TheConnectionString']

    @property
    def input_path(self):
        return self._input_path
    
    @property
    def target_path(self):
        return self._target_path
    
    @property
    def step(self):
        return self._step

    def download_data(self):

        logging.info("Downloading data from Blob Storage...")

        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        container_client = blob_service_client.get_container_client(container="cars")

        self.target_path.mkdir(exist_ok=True)
        files_in_dir = os.listdir(self.target_path)

        blob_list = container_client.list_blobs()
        count = 0
        for blob in blob_list:
            file_name = blob.name.replace(' ','_').replace(':',"")
            if file_name not in files_in_dir:
                download_file_path = self.target_path / Path(file_name)
                logging.info(f"Downloading file: {download_file_path}")
                with open(file=download_file_path, mode="wb") as download_file:
                    download_file.write(container_client.download_blob(blob.name).readall())
                count =+ 1

        if count: 
            logging.info(f"Step {self.step}: All files successfuly downloaded")
        else:
            logging.info(f"Step {self.step}: All files already in target directory.")

    def _iterate_items(self, items: list):
        pass