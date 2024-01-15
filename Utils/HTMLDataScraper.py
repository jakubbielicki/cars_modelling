import json
import logging
from .DataProcessor import DataProcessor
from pathlib import Path
from bs4 import BeautifulSoup

class HTMLDataScraper(DataProcessor):
    def __init__(self, params):
        super().__init__(params)
        self._input_path = self.root_path / Path(r"1_Raw_from_blob")
        self._target_path = self.root_path / Path(r"2_Webscrapped_data")
        self._step = 'SCRAP DATA'

    @property
    def input_path(self):
        return self._input_path
    
    @property
    def target_path(self):
        return self._target_path
    
    @property
    def step(self):
        return self._step

    def scrape_data(self):

        for file_name, file_contents in self.process_files():
            output_file_path = self.target_path / Path(file_name)
            with open(output_file_path, 'w') as file:
                json.dump(file_contents, file)

            logging.info(f"Webscrapped file {file_name} saved.")

    def _iterate_items(self, items: list, file_name: str):
        cars_prepared = []
        time_processed = file_name.replace('.json','')
        unable_to_scrap = 0
        for i, car_html in enumerate(items):
            try:
                scrapped_car_data = self.__scrape_page(car_html)
            except:
                logging.debug(f"Unable to scrap data from file {file_name}, index {i}")
                unable_to_scrap += 1
            else:
                car_model = scrapped_car_data['details']['Model pojazdu']
                seller = scrapped_car_data['seller']
                logging.debug(f"Data from file {time_processed}, car {car_model}, from seller {seller} scrapped.")
                cars_prepared.append(scrapped_car_data)

        logging.info(f"Cars from file {file_name} processed. Number of HTML file scrappings failed: {unable_to_scrap}")

        return cars_prepared
    
    def __scrape_page(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        scraped_data = {}

        #get offer name and price
        summary_div = soup.find('div', {'data-testid': 'summary-info-area'})
        offer_name = summary_div.select('[class*="offer-title big-text"]')[0].text
        price = summary_div.select('[class*="offer-price__number"]')[0].text
        scraped_data['offer_name'] = offer_name
        scraped_data['price'] = price

        #get seller name
        seller = soup.find('div', {'data-testid': 'aside-seller-info'}).find('div').find('div').find('p').text
        scraped_data['seller'] = seller

        #get offer details
        details_div = soup.find('div', {'data-testid': 'content-details-section'})
        details_div_list = details_div.find_all('div', {'data-testid': 'advert-details-item'})
        details = {}
        key = None
        value = None
        for detail_elements in details_div_list:
            for i, element in enumerate(detail_elements):
                if element.name in ['a', 'p']:
                    if i == 0:
                        key = element.text
                    else:
                        value = element.text
                if key and value:
                    details[key] = value
        scraped_data['details'] = details

        #get equipment info - not always there is one
        try:
            equipment = []
            equipment_lists = soup.select('[class*="accordion-item__collapse-inner"]')
            for equipment_list in equipment_lists:
                equipment_elements = equipment_list.find_all('p')
                for equipment_element in equipment_elements:
                    equipment.append(equipment_element.text)
        except:
            scraped_data['equipment'] = None
        else:
            scraped_data['equipment'] = equipment

        #get description - not always there is one
        try:
            description_div = soup.find('h3', text = 'Opis').find_parent('div')
            description = description_div.find('div').text
        except:
            scraped_data['description'] = None
        else:
            scraped_data['description'] = description

        return scraped_data
