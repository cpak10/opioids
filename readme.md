# Opioid Transactions in the United States
This repository contains tools to subset and analyze the opioid transactions in the United States. Requirements for running this program
can be found in the following file: requirements.txt

## Data
Data for opioid transactions in the United States from 2006 to 2014 is publicly available through court order. 
This data can be found at the following link: https://www.slcg.com/opioid-data/

The full transaction data is approximately 500 million observations and over 200 GB in size. Use the subset_data.py script to 
subset the full data into manageable portions. Use the create_table.sql to set up the table in a MySQL server.