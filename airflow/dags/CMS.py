import os
from datetime import datetime
import requests
import zipfile
import pandas as pd
import duckdb
import pyarrow as pa

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.task_group import TaskGroup

# Global configuration values
PROJECT_ID = "dtc-de-course-447715"
BUCKET = "cms_bucket_jj"
BIGQUERY_DATASET = "CMS"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
EXTRACT_PATH = os.path.join(path_to_local_home, "downloaded_files")
FILE_TYPES = ["RSRCH", "OWNRSHP", "GNRL"]

# URL template for the file to download (templated with execution_date)
url_template = "https://download.cms.gov/openpayments/PGYR{{ execution_date.strftime('%Y') }}_P01302025_01212025.zip"

def download_and_unzip(url, extract_path, **kwargs):
    """
    Download a zip file from the rendered URL and extract its contents.
    """
    execution_date = kwargs.get('execution_date')
    if execution_date:
        formatted_url = url.replace("{{ execution_date.strftime('%Y') }}", execution_date.strftime('%Y'))
    else:
        formatted_url = url

    os.makedirs(extract_path, exist_ok=True)
    print(f"Downloading file from {formatted_url}...")
    response = requests.get(formatted_url)
    if response.status_code != 200:
        raise Exception(f"Failed to download file from {formatted_url}")
    
    zip_path = os.path.join(extract_path, 'downloaded_file.zip')
    with open(zip_path, 'wb') as f:
        f.write(response.content)
    
    print("Extracting files...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    
    os.remove(zip_path)
    print("Download and extraction complete.")
    return extract_path

def list_files(extract_path):
    """
    List all CSV files found in the extraction directory.
    """
    csv_files = [f for f in os.listdir(extract_path) if f.endswith('.csv')]
    print("Found CSV files:", csv_files)
    return csv_files

def convert_dtl_ownrshp_to_parquet(extract_path, **kwargs):
    """
    Convert CSV files with 'DTL_OWNRSHP' in the filename to Parquet.
    """
    execution_date = kwargs.get('execution_date')
    year = execution_date.strftime('%Y')
    found_file = False
    
    for file in os.listdir(extract_path):
        if 'DTL_OWNRSHP' in file and file.endswith('.csv'):
            input_csv = os.path.join(extract_path, file)
            output_parquet = os.path.join(extract_path, f"OWNRSHP_{year}.parquet")
            print(f"Converting {input_csv} to Parquet...")
            df = pd.read_csv(input_csv)
            df.to_parquet(output_parquet, index=False)
            print(f"Converted {input_csv} to {output_parquet}")
            found_file = True
            

def convert_dtl_rsrch_to_parquet(extract_path, **kwargs):
    """
    Convert CSV files with 'DTL_RSRCH' in the filename to Parquet.
    """
    execution_date = kwargs.get('execution_date')
    year = execution_date.strftime('%Y')
    found_file = False
    
    for file in os.listdir(extract_path):
        if 'DTL_RSRCH' in file and file.endswith('.csv'):
            input_csv = os.path.join(extract_path, file)
            output_parquet = os.path.join(extract_path, f"RSRCH_{year}.parquet")
            print(f"Converting {input_csv} to Parquet...")
            dtype_dict_rsrch = {
        "Change_Type": str,
        "Covered_Recipient_Type": str,
        "Noncovered_Recipient_Entity_Name": str,
        "Teaching_Hospital_CCN": str,
        "Teaching_Hospital_ID": str,
        "Teaching_Hospital_Name": str,
        "Covered_Recipient_Profile_ID": str,
        "Covered_Recipient_NPI": str,
        "Covered_Recipient_First_Name": str,
        "Covered_Recipient_Middle_Name": str,
        "Covered_Recipient_Last_Name": str,
        "Covered_Recipient_Name_Suffix": str,
        "Recipient_Primary_Business_Street_Address_Line1": str,
        "Recipient_Primary_Business_Street_Address_Line2": str,
        "Recipient_City": str,
        "Recipient_State": str,
        "Recipient_Zip_Code": str,
        "Recipient_Country": str,
        "Recipient_Province": str,
        "Recipient_Postal_Code": str,
        "Covered_Recipient_Primary_Type_1": str,
        "Covered_Recipient_Primary_Type_2": str,
        "Covered_Recipient_Primary_Type_3": str,
        "Covered_Recipient_Primary_Type_4": str,
        "Covered_Recipient_Primary_Type_5": str,
        "Covered_Recipient_Primary_Type_6": str,
        "Covered_Recipient_Specialty_1": str,
        "Covered_Recipient_Specialty_2": str,
        "Covered_Recipient_Specialty_3": str,
        "Covered_Recipient_Specialty_4": str,
        "Covered_Recipient_Specialty_5": str,
        "Covered_Recipient_Specialty_6": str,
        "Covered_Recipient_License_State_code1": str,
        "Covered_Recipient_License_State_code2": str,
        "Covered_Recipient_License_State_code3": str,
        "Covered_Recipient_License_State_code4": str,
        "Covered_Recipient_License_State_code5": str,
        "Principal_Investigator_1_Covered_Recipient_Type": str,
        "Principal_Investigator_1_Profile_ID": str,
        "Principal_Investigator_1_NPI": str,
        "Principal_Investigator_1_First_Name": str,
        "Principal_Investigator_1_Middle_Name": str,
        "Principal_Investigator_1_Last_Name": str,
        "Principal_Investigator_1_Name_Suffix": str,
        "Principal_Investigator_1_Business_Street_Address_Line1": str,
        "Principal_Investigator_1_Business_Street_Address_Line2": str,
        "Principal_Investigator_1_City": str,
        "Principal_Investigator_1_State": str,
        "Principal_Investigator_1_Zip_Code": str,
        "Principal_Investigator_1_Country": str,
        "Principal_Investigator_1_Province": str,
        "Principal_Investigator_1_Postal_Code": str,
        "Principal_Investigator_1_Primary_Type_1": str,
        "Principal_Investigator_1_Primary_Type_2": str,
        "Principal_Investigator_1_Primary_Type_3": str,
        "Principal_Investigator_1_Primary_Type_4": str,
        "Principal_Investigator_1_Primary_Type_5": str,
        "Principal_Investigator_1_Primary_Type_6": str,
        "Principal_Investigator_1_Specialty_1": str,
        "Principal_Investigator_1_Specialty_2": str,
        "Principal_Investigator_1_Specialty_3": str,
        "Principal_Investigator_1_Specialty_4": str,
        "Principal_Investigator_1_Specialty_5": str,
        "Principal_Investigator_1_Specialty_6": str,
        "Principal_Investigator_1_License_State_code1": str,
        "Principal_Investigator_1_License_State_code2": str,
        "Principal_Investigator_1_License_State_code3": str,
        "Principal_Investigator_1_License_State_code4": str,
        "Principal_Investigator_1_License_State_code5": str,
        "Principal_Investigator_2_Covered_Recipient_Type": str,
        "Principal_Investigator_2_Profile_ID": str,
        "Principal_Investigator_2_NPI": str,
        "Principal_Investigator_2_First_Name": str,
        "Principal_Investigator_2_Middle_Name": str,
        "Principal_Investigator_2_Last_Name": str,
        "Principal_Investigator_2_Name_Suffix": str,
        "Principal_Investigator_2_Business_Street_Address_Line1": str,
        "Principal_Investigator_2_Business_Street_Address_Line2": str,
        "Principal_Investigator_2_City": str,
        "Principal_Investigator_2_State": str,
        "Principal_Investigator_2_Zip_Code": str,
        "Principal_Investigator_2_Country": str,
        "Principal_Investigator_2_Province": str,
        "Principal_Investigator_2_Postal_Code": str,
        "Principal_Investigator_2_Primary_Type_1": str,
        "Principal_Investigator_2_Primary_Type_2": str,
        "Principal_Investigator_2_Primary_Type_3": str,
        "Principal_Investigator_2_Primary_Type_4": str,
        "Principal_Investigator_2_Primary_Type_5": str,
        "Principal_Investigator_2_Primary_Type_6": str,
        "Principal_Investigator_2_Specialty_1": str,
        "Principal_Investigator_2_Specialty_2": str,
        "Principal_Investigator_2_Specialty_3": str,
        "Principal_Investigator_2_Specialty_4": str,
        "Principal_Investigator_2_Specialty_5": str,
        "Principal_Investigator_2_Specialty_6": str,
        "Principal_Investigator_2_License_State_code1": str,
        "Principal_Investigator_2_License_State_code2": str,
        "Principal_Investigator_2_License_State_code3": str,
        "Principal_Investigator_2_License_State_code4": str,
        "Principal_Investigator_2_License_State_code5": str,
        "Principal_Investigator_3_Covered_Recipient_Type": str,
        "Principal_Investigator_3_Profile_ID": str,
        "Principal_Investigator_3_NPI": str,
        "Principal_Investigator_3_First_Name": str,
        "Principal_Investigator_3_Middle_Name": str,
        "Principal_Investigator_3_Last_Name": str,
        "Principal_Investigator_3_Name_Suffix": str,
        "Principal_Investigator_3_Business_Street_Address_Line1": str,
        "Principal_Investigator_3_Business_Street_Address_Line2": str,
        "Principal_Investigator_3_City": str,
        "Principal_Investigator_3_State": str,
        "Principal_Investigator_3_Zip_Code": str,
        "Principal_Investigator_3_Country": str,
        "Principal_Investigator_3_Province": str,
        "Principal_Investigator_3_Postal_Code": str,
        "Principal_Investigator_3_Primary_Type_1": str,
        "Principal_Investigator_3_Primary_Type_2": str,
        "Principal_Investigator_3_Primary_Type_3": str,
        "Principal_Investigator_3_Primary_Type_4": str,
        "Principal_Investigator_3_Primary_Type_5": str,
        "Principal_Investigator_3_Primary_Type_6": str,
        "Principal_Investigator_3_Specialty_1": str,
        "Principal_Investigator_3_Specialty_2": str,
        "Principal_Investigator_3_Specialty_3": str,
        "Principal_Investigator_3_Specialty_4": str,
        "Principal_Investigator_3_Specialty_5": str,
        "Principal_Investigator_3_Specialty_6": str,
        "Principal_Investigator_3_License_State_code1": str,
        "Principal_Investigator_3_License_State_code2": str,
        "Principal_Investigator_3_License_State_code3": str,
        "Principal_Investigator_3_License_State_code4": str,
        "Principal_Investigator_3_License_State_code5": str,
        "Principal_Investigator_4_Covered_Recipient_Type": str,
        "Principal_Investigator_4_Profile_ID": str,
        "Principal_Investigator_4_NPI": str,
        "Principal_Investigator_4_First_Name": str,
        "Principal_Investigator_4_Middle_Name": str,
        "Principal_Investigator_4_Last_Name": str,
        "Principal_Investigator_4_Name_Suffix": str,
        "Principal_Investigator_4_Business_Street_Address_Line1": str,
        "Principal_Investigator_4_Business_Street_Address_Line2": str,
        "Principal_Investigator_4_City": str,
        "Principal_Investigator_4_State": str,
        "Principal_Investigator_4_Zip_Code": str,
        "Principal_Investigator_4_Country": str,
        "Principal_Investigator_4_Province": str,
        "Principal_Investigator_4_Postal_Code": str,
        "Principal_Investigator_4_Primary_Type_1": str,
        "Principal_Investigator_4_Primary_Type_2": str,
        "Principal_Investigator_4_Primary_Type_3": str,
        "Principal_Investigator_4_Primary_Type_4": str,
        "Principal_Investigator_4_Primary_Type_5": str,
        "Principal_Investigator_4_Primary_Type_6": str,
        "Principal_Investigator_4_Specialty_1": str,
        "Principal_Investigator_4_Specialty_2": str,
        "Principal_Investigator_4_Specialty_3": str,
        "Principal_Investigator_4_Specialty_4": str,
        "Principal_Investigator_4_Specialty_5": str,
        "Principal_Investigator_4_Specialty_6": str,
        "Principal_Investigator_4_License_State_code1": str,
        "Principal_Investigator_4_License_State_code2": str,
        "Principal_Investigator_4_License_State_code3": str,
        "Principal_Investigator_4_License_State_code4": str,
        "Principal_Investigator_4_License_State_code5": str,
        "Principal_Investigator_5_Covered_Recipient_Type": str,
        "Principal_Investigator_5_Profile_ID": str,
        "Principal_Investigator_5_NPI": str,
        "Principal_Investigator_5_First_Name": str,
        "Principal_Investigator_5_Middle_Name": str,
        "Principal_Investigator_5_Last_Name": str,
        "Principal_Investigator_5_Name_Suffix": str,
        "Principal_Investigator_5_Business_Street_Address_Line1": str,
        "Principal_Investigator_5_Business_Street_Address_Line2": str,
        "Principal_Investigator_5_City": str,
        "Principal_Investigator_5_State": str,
        "Principal_Investigator_5_Zip_Code": str,
        "Principal_Investigator_5_Country": str,
        "Principal_Investigator_5_Province": str,
        "Principal_Investigator_5_Postal_Code": str,
        "Principal_Investigator_5_Primary_Type_1": str,
        "Principal_Investigator_5_Primary_Type_2": str,
        "Principal_Investigator_5_Primary_Type_3": str,
        "Principal_Investigator_5_Primary_Type_4": str,
        "Principal_Investigator_5_Primary_Type_5": str,
        "Principal_Investigator_5_Primary_Type_6": str,
        "Principal_Investigator_5_Specialty_1": str,
        "Principal_Investigator_5_Specialty_2": str,
        "Principal_Investigator_5_Specialty_3": str,
        "Principal_Investigator_5_Specialty_4": str,
        "Principal_Investigator_5_Specialty_5": str,
        "Principal_Investigator_5_Specialty_6": str,
        "Principal_Investigator_5_License_State_code1": str,
        "Principal_Investigator_5_License_State_code2": str,
        "Principal_Investigator_5_License_State_code3": str,
        "Principal_Investigator_5_License_State_code4": str,
        "Principal_Investigator_5_License_State_code5": str,
        "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name": str,
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID": int,
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name": str,
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State": str,
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country": str,
        "Related_Product_Indicator": str,
        "Covered_or_Noncovered_Indicator_1": str,
        "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1": str,
        "Product_Category_or_Therapeutic_Area_1": str,
        "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1": str,
        "Associated_Drug_or_Biological_NDC_1": str,
        "Associated_Device_or_Medical_Supply_PDI_1": str,
        "Covered_or_Noncovered_Indicator_2": str,
        "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2": str,
        "Product_Category_or_Therapeutic_Area_2": str,
        "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2": str,
        "Associated_Drug_or_Biological_NDC_2": str,
        "Associated_Device_or_Medical_Supply_PDI_2": str,
        "Covered_or_Noncovered_Indicator_3": str,
        "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3": str,
        "Product_Category_or_Therapeutic_Area_3": str,
        "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3": str,
        "Associated_Drug_or_Biological_NDC_3": str,
        "Associated_Device_or_Medical_Supply_PDI_3": str,
        "Covered_or_Noncovered_Indicator_4": str,
        "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4": str,
        "Product_Category_or_Therapeutic_Area_4": str,
        "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4": str,
        "Associated_Drug_or_Biological_NDC_4": str,
        "Associated_Device_or_Medical_Supply_PDI_4": str,
        "Covered_or_Noncovered_Indicator_5": str,
        "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5": str,
        "Product_Category_or_Therapeutic_Area_5": str,
        "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5": str,
        "Associated_Drug_or_Biological_NDC_5": str,
        "Associated_Device_or_Medical_Supply_PDI_5": str,
        "Total_Amount_of_Payment_USDollars": str,
        "Date_of_Payment": str,
        "Form_of_Payment_or_Transfer_of_Value": str,
        "Expenditure_Category1": str,
        "Expenditure_Category2": str,
        "Expenditure_Category3": str,
        "Expenditure_Category4": str,
        "Expenditure_Category5": str,
        "Expenditure_Category6": str,
        "Preclinical_Research_Indicator": str,
        "Delay_in_Publication_Indicator": str,
        "Name_of_Study": str,
        "Dispute_Status_for_Publication": str,
        "Record_ID": int,
        "Program_Year": int,
        "Payment_Publication_Date": str,
        "ClinicalTrials_Gov_Identifier": str,
        "Research_Information_Link": str,
        "Context_of_Research": str
            }
            df = pd.read_csv(input_csv, dtype=dtype_dict_rsrch)
            df.to_parquet(output_parquet, index=False)
            print(f"Converted {input_csv} to {output_parquet}")
            found_file = True

def convert_large_dtl_gnrl_to_parquet(extract_path, **kwargs):
    """
    Convert CSV files with 'DTL_GNRL' in the filename to Parquet using DuckDB.
    """
    dtype_dict_gnrl = {
        "Change_Type": "object",
        "Covered_Recipient_Type": "object",
        "Teaching_Hospital_CCN": "object",
        "Teaching_Hospital_ID": "object",
        "Teaching_Hospital_Name": "object",
        "Covered_Recipient_Profile_ID": "object",
        "Covered_Recipient_NPI": "object",
        "Covered_Recipient_First_Name": "object",
        "Covered_Recipient_Middle_Name": "object",
        "Covered_Recipient_Last_Name": "object",
        "Covered_Recipient_Name_Suffix": "object",
        "Recipient_Primary_Business_Street_Address_Line1": "object",
        "Recipient_Primary_Business_Street_Address_Line2": "object",
        "Recipient_City": "object",
        "Recipient_State": "object",
        "Recipient_Zip_Code": "object",
        "Recipient_Country": "object",
        "Recipient_Province": "object",
        "Recipient_Postal_Code": "object",
        "Covered_Recipient_Primary_Type_1": "object",
        "Covered_Recipient_Primary_Type_2": "object",
        "Covered_Recipient_Primary_Type_3": "object",
        "Covered_Recipient_Primary_Type_4": "object",
        "Covered_Recipient_Primary_Type_5": "object",
        "Covered_Recipient_Primary_Type_6": "object",
        "Covered_Recipient_Specialty_1": "object",
        "Covered_Recipient_Specialty_2": "object",
        "Covered_Recipient_Specialty_3": "object",
        "Covered_Recipient_Specialty_4": "object",
        "Covered_Recipient_Specialty_5": "object",
        "Covered_Recipient_Specialty_6": "object",
        "Covered_Recipient_License_State_code1": "object",
        "Covered_Recipient_License_State_code2": "object",
        "Covered_Recipient_License_State_code3": "object",
        "Covered_Recipient_License_State_code4": "object",
        "Covered_Recipient_License_State_code5": "object",
        "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name": "object",
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID": "object",
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name": "object",
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State": "object",
        "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country": "object",
        "Total_Amount_of_Payment_USDollars": "object",
        "Date_of_Payment": "object",
        "Number_of_Payments_Included_in_Total_Amount": "int64",
        "Form_of_Payment_or_Transfer_of_Value": "object",
        "Nature_of_Payment_or_Transfer_of_Value": "object",
        "City_of_Travel": "object",
        "State_of_Travel": "object",
        "Country_of_Travel": "object",
        "Physician_Ownership_Indicator": "object",
        "Third_Party_Payment_Recipient_Indicator": "object",
        "Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value": "object",
        "Charity_Indicator": "object",
        "Third_Party_Equals_Covered_Recipient_Indicator": "object",
        "Contextual_Information": "object",
        "Delay_in_Publication_Indicator": "object",
        "Record_ID": "object",
        "Dispute_Status_for_Publication": "object",
        "Related_Product_Indicator": "object",
        "Covered_or_Noncovered_Indicator_1": "object",
        "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1": "object",
        "Product_Category_or_Therapeutic_Area_1": "object",
        "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1": "object",
        "Associated_Drug_or_Biological_NDC_1": "object",
        "Associated_Device_or_Medical_Supply_PDI_1": "object",
        "Covered_or_Noncovered_Indicator_2": "object",
        "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2": "object",
        "Product_Category_or_Therapeutic_Area_2": "object",
        "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2": "object",
        "Associated_Drug_or_Biological_NDC_2": "object",
        "Associated_Device_or_Medical_Supply_PDI_2": "object",
        "Covered_or_Noncovered_Indicator_3": "object",
        "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3": "object",
        "Product_Category_or_Therapeutic_Area_3": "object",
        "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3": "object",
        "Associated_Drug_or_Biological_NDC_3": "object",
        "Associated_Device_or_Medical_Supply_PDI_3": "object",
        "Covered_or_Noncovered_Indicator_4": "object",
        "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4": "object",
        "Product_Category_or_Therapeutic_Area_4": "object",
        "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4": "object",
        "Associated_Drug_or_Biological_NDC_4": "object",
        "Associated_Device_or_Medical_Supply_PDI_4": "object",
        "Covered_or_Noncovered_Indicator_5": "object",
        "Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5": "object",
        "Product_Category_or_Therapeutic_Area_5": "object",
        "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5": "object",
        "Associated_Drug_or_Biological_NDC_5": "object",
        "Associated_Device_or_Medical_Supply_PDI_5": "object",
        "Program_Year": "int64",
        "Payment_Publication_Date": "object"
    }
    # Mapping Python types to DuckDB SQL types
    type_mapping = {'object': 'VARCHAR', 'float64': 'DOUBLE', 'int64': 'BIGINT'}
    execution_date = kwargs.get("execution_date")
    for file in os.listdir(extract_path):
        if 'DTL_GNRL' in file and file.endswith('.csv'):
            input_csv = os.path.join(extract_path, file)
            output_parquet = os.path.join(extract_path, f"GNRL_{execution_date.strftime('%Y')}.parquet")
            print(f"Converting {input_csv} to Parquet using DuckDB...")
            conn = duckdb.connect(database=':memory:')
            schema_parts = []
            for col, dtype in dtype_dict_gnrl.items():
                sql_type = type_mapping.get(dtype, 'VARCHAR')
                schema_parts.append(f'"{col}": {sql_type}')
            schema_str = '{' + ', '.join(schema_parts) + '}'
            query = f"""
                COPY (
                    SELECT * FROM read_csv_auto(
                        '{input_csv}', 
                        header=true,
                        sample_size=10000,
                        strict_mode=false,
                        ignore_errors=true,
                        quote=''
                    )
                )
                TO '{output_parquet}' (FORMAT PARQUET);
            """
            conn.execute(query)
            print(f"Converted {input_csv} to {output_parquet}")

def upload_to_gcs_for_file(bucket, extract_path, **kwargs):
    """
    Upload the Parquet file for a given file_type to GCS.
    """
    execution_date = kwargs.get('execution_date')
    # Ensure execution_date is a datetime object.
    if not hasattr(execution_date, 'strftime'):
        execution_date = datetime.fromisoformat(execution_date)
    
    year = execution_date.strftime('%Y')
    file_type = kwargs.get('file_type')
    file_name = f"{file_type}_{year}.parquet"
    local_file = os.path.join(extract_path, file_name)
    
    # Print more debug info
    print(f"Looking for file: {local_file}")
    print(f"File exists: {os.path.exists(local_file)}")
    
    if not os.path.exists(local_file):
        print(f"File {local_file} does not exist. Listing directory contents:")
        for file in os.listdir(extract_path):
            print(f"  - {file}")
        raise Exception(f"{local_file} does not exist")
    
    object_name = f"raw/{file_name}"
    print(f"Uploading {local_file} to gs://{bucket}/{object_name}...")
    hook = GCSHook(gcp_conn_id="gcp-airflow")
    hook.upload(
        bucket_name=bucket,
        object_name=object_name,
        filename=local_file,
        timeout=6000
    )
    print(f"Uploaded {local_file} to gs://{bucket}/{object_name}")
    return file_name

default_args = {
    "start_date": datetime(2017, 1, 1),
    "end_date": datetime(2019, 12, 31),
    "retries": 10,
}

with DAG(
    "GCP_ingestion_CMS",
    schedule_interval="0 0 1 1 *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    template_searchpath=[path_to_local_home],
) as dag:

    # Step 1: Download and unzip the file
    download_task = PythonOperator(
        task_id="download_and_unzip",
        python_callable=download_and_unzip,
        op_kwargs={'url': url_template, 'extract_path': EXTRACT_PATH},
        provide_context=True,
    )

    # Step 2: List files after extraction
    list_task = PythonOperator(
        task_id="list_files",
        python_callable=list_files,
        op_kwargs={'extract_path': EXTRACT_PATH},
        provide_context=True,
    )

    # Step 3: Convert CSVs to Parquet (sequentially)
    ownrshp_to_parquet_task = PythonOperator(
        task_id="convert_dtl_ownrshp_to_parquet",
        python_callable=convert_dtl_ownrshp_to_parquet,
        op_kwargs={'extract_path': EXTRACT_PATH},
        provide_context=True,
    )

    rsrch_to_parquet_task = PythonOperator(
        task_id="convert_dtl_rsrch_to_parquet",
        python_callable=convert_dtl_rsrch_to_parquet,
        op_kwargs={'extract_path': EXTRACT_PATH},
        provide_context=True,
    )

    gnrl_to_parquet_task = PythonOperator(
        task_id="convert_large_dtl_gnrl_to_parquet",
        python_callable=convert_large_dtl_gnrl_to_parquet,
        op_kwargs={'extract_path': EXTRACT_PATH},
        provide_context=True,
    )

    # Step 4: Group file type tasks (upload to GCS and BigQuery operations) using TaskGroup
    with TaskGroup(group_id="bigquery_processing") as bq_group:
        for file_type in FILE_TYPES:
            with TaskGroup(group_id=f"{file_type}_group") as file_group:
                # Upload task for the current file type
                upload_task = PythonOperator(
                    task_id=f"upload_to_gcs_{file_type}",
                    python_callable=upload_to_gcs_for_file,
                    op_kwargs={
                        'bucket': BUCKET,
                        'extract_path': EXTRACT_PATH,
                        'file_type': file_type
                    },
                    provide_context=True,
                )
                # For BigQuery tasks, use a templated year string
                year_template = "{{ execution_date.strftime('%Y') }}"
                file_name_parquet = f"{file_type}_{year_template}.parquet"

                # Create external table (year-specific)
                create_external_table = BigQueryInsertJobOperator(
                    task_id=f"create_external_table_{file_type}",
                    gcp_conn_id="gcp-airflow",
                    configuration={
                        "query": {
                            "query": f"""
                                CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{file_type}_{year_template}_ext`
                                OPTIONS (
                                    uris = ['gs://{BUCKET}/raw/{file_name_parquet}'],
                                    format = 'PARQUET'
                                );
                            """,
                            "useLegacySql": False,
                        }
                    },
                    retries=3,
                )

                # Create final table (without year; created only if not exists)
                create_final_table = BigQueryInsertJobOperator(
                    task_id=f"create_final_table_{file_type}",
                    gcp_conn_id="gcp-airflow",
                    configuration={
                        "query": {
                            "query": f"""
                                CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{BIGQUERY_DATASET}.{file_type}_all`
                                AS
                                SELECT 
                                    '{file_name_parquet}' AS filename,
                                    * 
                                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{file_type}_{year_template}_ext`
                                WHERE 1=0;
                            """,
                            "useLegacySql": False,
                        }
                    },
                    retries=3,
                )

                # Create temporary table (year-specific)
                create_temp_table = BigQueryInsertJobOperator(
                    task_id=f"create_temp_table_{file_type}",
                    gcp_conn_id="gcp-airflow",
                    configuration={
                        "query": {
                            "query": f"""
                                CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{file_type}_{year_template}_tmp`
                                AS
                                SELECT
                                    '{file_name_parquet}' AS filename,
                                    *
                                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{file_type}_{year_template}_ext`;
                            """,
                            "useLegacySql": False,
                        }
                    },
                    retries=3,
                )
            
                # Merge temporary table into the final table
                '''
                merge_to_final_table = BigQueryInsertJobOperator(
                    task_id=f"merge_to_final_table_{file_type}",
                    gcp_conn_id="gcp-airflow",
                    configuration={
                        "query": {
                            "query": f"""
                                INSERT INTO `{PROJECT_ID}.{BIGQUERY_DATASET}.{file_type}_all` 
                                SELECT *
                                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{file_type}_{year_template}_tmp` 
                            """,
                            "useLegacySql": False,
                        }
                    },
                    retries=3,
                ) '''

                # Chain the tasks for this file type sequentially
                upload_task >> create_external_table >> create_final_table >> create_temp_table
    # Step 5: Cleanup local files after all processing is complete
    cleanup_task = BashOperator(
        task_id="cleanup_files",
        bash_command=f"rm -rf {EXTRACT_PATH}",
    )

    # Define overall task sequence
    download_task >> list_task >> ownrshp_to_parquet_task >> rsrch_to_parquet_task >> gnrl_to_parquet_task >> bq_group >> cleanup_task