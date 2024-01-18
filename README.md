This python project is a data pipeline responsible for processing all sale adversements on VW T-Cross from Polish car sales portal
and creating car price predction model basing on this data.

Car sale ads are being downloaded on daily basis with Azure Function and saved on Azure Blob storage,
which is done outside this project. Then, code in this project is responsible for the following:
- Getting daily HTML data from Azure Blob Storage and saving it locally (BlobDataDownloader object)
- Webscrapping data from daily HTML data and saving it into daily JSON data (HTMLDataScraper object)
- Structurizing daily JSON data into one summary CSV file (CSVDataStructurizer object)
- Deduplicating and cleaning data from summary CSV file (CSVDataCleanser object)
- Adjusting clean data for modelling purposes (CSVDataAdjustor object)

Before running the pipeline the following needs to be done:
- Azure Blob Storage connection string should be saved as environment variable "TheConnectionString"
- config.yaml file parameters should be set (note, that root_path and logging_level don't need to be set)

The pipeline can be executed by running Entry_point.py file.

Once the pipeline has been run, there are following manual steps to take:
- Exploring structurized data (_Data_exploration.ipynb notebook)
- Creating price prediction model (_Modelling.ipynb notebook)