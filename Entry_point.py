from Utils import *

def main():

    params = load_parameters('config.yaml')

    #downloader = BlobDataDownloader(params)
    #scraper = HTMLDataScraper(params)
    #structurizer = CSVDataStructurizer(params)
    #cleanser = CSVDataCleanser(params)
    adjustor = CSVDataAdjustor(params)

    # Execute the required processes
    #downloader.download_data()
    #scraper.scrape_data()
    #structurizer.structure_data()
    #cleanser.cleanse_data()
    adjustor.adjust_data()

if __name__ == "__main__":
    main()