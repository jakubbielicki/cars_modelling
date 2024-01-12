# cars_modelling

This contain the python code, that is responsible for doing the following:
- Getting daily HTML data from Azure Blob Storage and saving it locally
- Webscrapping data from all HTML files
- Saving webscrapped data in CSV file

Code is steered from Entry_point.py file, my_main function.
In the function, you can choose paths to save files. 

To be revised:
- changing the way data is saved (only one car per json file) + adding concurency
- adding method chaining and separating writing from going through a file
- add translating and schema to config
