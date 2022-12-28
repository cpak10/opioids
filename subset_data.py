import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode

# define the filters
city = "BEVERLY HILLS"
state = "CA"
county = "LOS ANGELES"
table_name = "arcos_ca_los_angeles_beverly_hills"

# define mysql database and tables
db_name = "opioids"
table = f"""
    create table {table_name}(
        id int not null,
        reporter_dea_no varchar(256), reporter_bus_act varchar(256), reporter_name varchar(256), 
        reporter_addl_co_info varchar(256), reporter_address1 varchar(256), reporter_address2 varchar(256),
        reporter_city varchar(256), reporter_state varchar(256), reporter_zip varchar(256), reporter_county varchar(256),
        buyer_dea_no varchar(256), buyer_bus_act varchar(256), buyer_name varchar(256),
        buyer_addl_co_info varchar(256), buyer_address1 varchar(256), buyer_address2 varchar(256),
        buyer_city varchar(256), buyer_state varchar(256), buyer_zip varchar(256), buyer_county varchar(256),
        transaction_code varchar(256), drug_code varchar(256), ndc_no varchar(256), drug_name varchar(256),
        quantity float(2), unit varchar(256), action_indicator varchar(256), order_form_no varchar(256),
        correction_no varchar(256), strength varchar(256), transaction_date varchar(256), calc_base_wt_in_gm float(2),
        dosage_unit varchar(256), transaction_id varchar(256), product_name varchar(256), ingredient_name varchar(256),
        measure varchar(256), mme_conversion_factor float(2), combined_labeler_name varchar(256), 
        revised_company_name varchar(256), reporter_family varchar(256), dos_str varchar(256), 
        primary key (id)
    )
"""

# ignore the error from unstringable values in the data
np.seterr(invalid = "ignore")

# set up mysql connection
cnx = mysql.connector.connect(user = "USER", password = "PASSWORD")
cursor = cnx.cursor()

# check for database and create database if does not exist
def create_database(cursor):
    try:
        cursor.execute(f"create database {db_name}")
    except mysql.connector.Error as err:
        print(f"ERROR: Failed creating database: {err}")
        exit(1)

try:
    cursor.execute(f"use {db_name}")
except mysql.connector.Error as err:
    print(f"\nNOTE: Database {db_name} does not exists.")
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print(f"NOTE: Database {db_name} created successfully.")
        cnx.database = db_name
    else:
        print(err)
        exit(1)

# drop table and create fresh table
cursor.execute(f"drop table if exists {table_name}")
cursor.execute(table)

# loop through the full data in manageable chunks
n_chunk = 0
n_observations = 0
for chunk in pd.read_csv("C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\ARCOS US 14 Drugs Nov 2020.csv", sep = "|", iterator = True, chunksize = 10000000,
    # set the datatypes for each column explicitly
    dtype = {
        "REPORTER_DEA_NO": "object", "REPORTER_BUS_ACT": "object", "REPORTER_NAME": "object",
        "REPORTER_ADDL_CO_INFO": "object", "REPORTER_ADDRESS1": "object", "REPORTER_ADDRESS2": "object",
        "REPORTER_CITY": "object", "REPORTER_STATE": "object", "REPORTER_ZIP": "object", "REPORTER_COUNTY": "object",
        "BUYER_DEA_NO": "object", "BUYER_BUS_ACT": "object", "BUYER_NAME": "object",
        "BUYER_ADDL_CO_INFO": "object", "BUYER_ADDRESS1": "object", "BUYER_ADDRESS2": "object",
        "BUYER_CITY": "object", "BUYER_STATE": "object", "BUYER_ZIP": "object", "BUYER_COUNTY": "object",
        "TRANSACTION_CODE": "object", "DRUG_CODE": "object", "NDC_NO": "object", "DRUG_NAME": "object",
        "QUANTITY": "float64", "UNIT": "object", "ACTION_INDICATOR": "object", "ORDER_FORM_NO": "object",
        "CORRECTION_NO": "object", "STRENGTH": "object", "TRANSACTION_DATE": "object", "CALC_BASE_WT_IN_GM": "float64",
        "DOSAGE_UNIT": "object", "TRANSACTION_ID": "object", "Product_Name": "object", "Ingredient_Name": "object", 
        "Measure": "object", "MME_Conversion_Factor": "float64", "Combined_Labeler_Name": "object", 
        "Revised_Company_Name": "object", "Reporter_family": "object", "dos_str": "object"
    }
):
    chunk_sub = chunk[(chunk["BUYER_CITY"] == city) & (chunk["BUYER_STATE"] == state) & (chunk["BUYER_COUNTY"] == county)]
    if chunk_sub[["CALC_BASE_WT_IN_GM", "QUANTITY", "MME_Conversion_Factor"]].isna().any().any() == True:
        print("\nERROR: Missing values in numeric columns")
        exit(1)
    chunk_sub_fill_missing = chunk_sub.fillna("")
    for i, row in chunk_sub_fill_missing.iterrows():
        row_tuple = tuple(row)
        row_add_index = (n_observations,) + row_tuple
        sql = f"""insert into {table_name} values(
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        )"""
        cursor.execute(sql, tuple(row_add_index))
        cnx.commit()
        n_observations += 1

    n_chunk += 1
    print(f"\nNOTE: Processed {n_chunk * 10} million observations")
    print(f"NOTE: Added {chunk_sub.shape[0]} observations to table")
    print(f"NOTE: Table is {n_observations} observations")

# closing mysql connection
cursor.close()
cnx.close()