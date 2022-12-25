import pandas as pd
import numpy as np

# define the filters
city = "BEVERLY HILLS"
state = "CA"
county = "LOS ANGELES"

# ignore the error from unstringable values in the data
np.seterr(invalid = "ignore")

# set up the subset dataset
arcos_sub = pd.DataFrame()

# loop through the full data in manageable chunks
n_chunk = 0
for chunk in pd.read_csv("C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\ARCOS US 14 Drugs Nov 2020.csv", sep = "|", iterator = True, chunksize = 20000000,
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
        "CORRECTION_NO": "object", "STRENGTH": "float64", "TRANSACTION_DATE": "object", "CALC_BASE_WT_IN_GM": "float64",
        "DOSAGE_UNIT": "float64", "TRANSACTION_ID": "object", "Product_Name": "object", "Ingredient_Name": "object", 
        "Measure": "object", "MME_Conversion_Factor": "float64", "Combined_Labeler_Name": "object", 
        "Revised_Company_Name": "object", "Reporter_family": "object", "dos_str": "float64"
    }
):
    chunk_sub = chunk[(chunk["BUYER_CITY"] == city) & (chunk["BUYER_STATE"] == state) & (chunk["BUYER_COUNTY"] == county)]
    arcos_sub = pd.concat([arcos_sub, chunk_sub], ignore_index = True)
    n_chunk = n_chunk + 1
    print(f"\nNOTE: Processed {n_chunk * 20} million observations")
    print(f"NOTE: Subset is {arcos_sub.shape[0]} observations")

# send the subset to csv
arcos_sub.to_csv(f"C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\ARCOS_{state}_{county}_{city}.csv", sep = "|")