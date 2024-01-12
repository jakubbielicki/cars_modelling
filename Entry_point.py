from Utils import load_parameters, BlobDataDownloader, HTMLDataScraper, CSVDataStructurizer

def main():

    params = load_parameters('config.yaml')

    #downloader = BlobDataDownloader(params)
    #scraper = HTMLDataScraper(params)
    structurizer = CSVDataStructurizer(params)

    # Execute the required processes
    #downloader.download_data()
    #scraper.scrape_data()
    structurizer.structure_data()

if __name__ == "__main__":
    main()