# Opioid Transactions in the United States
This repository contains tools to subset and analyze the opioid transactions in the United States. Requirements for running this program
can be found in the following file: requirements.txt

## Data
Data for opioid transactions in the United States from 2006 to 2014 is publicly available through court order ("ARCOS"). 
This data can be found at the following link: https://www.slcg.com/opioid-data/

The full transaction data is approximately 500 million observations and over 200 GB in size.

## Deployment
* subset_data.py: 
This script assumes that the ARCOS data referenced above is downloaded and a connection to a MySQL server is ready. 
The script will filter the ARCOS data and commit the data into a new table in MySQL. 
The file path for the ARCOS data will need to be updated based on local save location. 
Additionally, the batch size for the pandas loop may need to be updated based on local RAM availabilities.