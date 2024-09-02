import time
import pandas as pd
import os
from google.cloud import bigquery
import re
import argparse
import datetime
import string
import numpy as np
from tqdm import tqdm

kSDLC_TEMP_CSV = "gbq_tempFile.csv"

gbq_details = {
    "project_id": 'project_id',
    "dataset_id": 'dataset_id',
}

def table_exists(client, dataset_id, table_id):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
        return True
    except Exception as e:
        return False
    
def clean_excel_data(df):
    for column in df.columns:
        if df[column].dtype == object:
            df[column] = df[column].apply(lambda x: ' '.join(x.split()) if isinstance(x, str) else x)
    return df

def display_column_info(csv_file): 
    try:
        df = pd.read_csv(csv_file)  
        df.columns = [col.strip().upper().translate(str.maketrans("", "", string.punctuation)) for col in df.columns]
        df.columns = [re.sub('\s+', '_', col) for col in df.columns]
        
        column_info = df.dtypes
        original_data_types = {col: column_info[col] for col in df.columns}
        print("Column Names and Data Types (GBQ):")
        print("__________________________________________________________________________________________________________________")
        print("__________________________________________________________________________________________________________________")
        for i, (col, data_type) in enumerate(column_info.items(), 1):
            print(f"{i}. {col}: {data_type}")
        return df, original_data_types
    except Exception as e:
        print(f"Error displaying column information: {e}")
        return None, None

def modify_column_data_types(df, original_data_types):
    acceptable_data_types = ["int", "float", "string", "timestamp", "date"]
    custom_data_types = {}
    print("__________________________________________________________________________________________________________________")

    while True:
        choice = input("Enter the column index to modify or 'exit' to finish: ").strip()
        if choice == 'exit':
            break

        try:
            col_index = int(choice) - 1
            if 0 <= col_index < len(df.columns):
                col = df.columns[col_index]
                default_data_type = map_to_gbq_data_type(original_data_types[col])
                data_type = input(f"Specify data type for column '{col}' (default: {default_data_type}, acceptable: [int, float, string, timestamp, date]): ").strip()

                if data_type in acceptable_data_types:
                    if data_type == 'int' and default_data_type == 'FLOAT':
                        try:
                            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(np.int64)
                        except ValueError as e:
                            print(f"Error converting column '{col}' to integer: {e}")
                            custom_data_types[col] = default_data_type
                            continue
                    elif data_type == 'int':
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(np.int64)
                    elif data_type == 'float' and default_data_type == 'INT':
                        df[col] = df[col].astype('float')
                    elif data_type == 'date':
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                    elif data_type == 'timestamp':
                         df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                    custom_data_types[col] = "INTEGER" if data_type == "int" else data_type
                else:
                    custom_data_types[col] = default_data_type
            else:
                print("Invalid column index. Please choose a valid index.")
        except ValueError as exception:
            print(f"Error processing column: {exception}")
            print("Invalid input. Please enter a valid column index or 'exit' to finish.")
            
    print("__________________________________________________________________________________________________________________")
    print("Modified Data Types:")
    for col, data_type in custom_data_types.items():
        print(f"{col}: {data_type}")

    return custom_data_types

def map_to_gbq_data_type(pandas_data_type):
    return {
        "int64": "INTEGER",
        "float64": "FLOAT",
        "object": "STRING",
        "datetime64[ns]": "DATE",
        "timestamp": "TIMESTAMP",
    }.get(pandas_data_type.name, "STRING")


def create_table_sql(df, custom_data_types, table_name):
    schema = []
    for col in df.columns:
        data_type = custom_data_types.get(col, map_to_gbq_data_type(df[col].dtype))
        schema.append(bigquery.SchemaField(col, data_type))

    table_id = f"{gbq_details['project_id']}.{gbq_details['dataset_id']}.{table_name}"
    table_ref = bigquery.Client().dataset(gbq_details['dataset_id']).table(table_name)
    table = bigquery.Table(table_ref, schema=schema)

    if table_exists(bigquery.Client(), gbq_details["dataset_id"], table_name):
        bigquery.Client().delete_table(table_ref)

    bigquery.Client().create_table(table)

    return table

def create_and_upload_gbq_table(excel_file, table_name): 
    csv_file = kSDLC_TEMP_CSV
    df = pd.read_excel(excel_file)
    df.to_csv(csv_file, index=False)

    df, original_data_types = display_column_info(csv_file)  
    df = clean_excel_data(df)
    custom_data_types = modify_column_data_types(df, original_data_types)
    
    schema = create_table_sql(df, custom_data_types, table_name)

    df.to_csv(kSDLC_TEMP_CSV, index=False, sep='|')

    job_config = bigquery.LoadJobConfig(
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter='|',
    )

    try:
        with open(kSDLC_TEMP_CSV, "rb") as source_file:
            job = bigquery.Client().load_table_from_file(source_file, schema, job_config=job_config)
            job.result() 
            print(f'Loaded {job.output_rows} rows into {schema.dataset_id}.{schema.table_id}')
    except Exception as e:
        print("Error uploading data to Google BigQuery table.", e)
        for e in job.errors:
            print(e)           
    csv_records = df.shape[0]
    print(f"Number of records in the CSV file: {csv_records}")

if __name__ == "__main__":
    starttime = time.time()

    sdlc_parser = argparse.ArgumentParser(prog='GBQUtil')
    sdlc_parser.add_argument("--excelFile", "-excel", help="Specify Excel file to convert and upload to Google BigQuery")  
    sdlc_parser.add_argument("--tableName", "-table", help="Specify Google BigQuery table name")
    args = sdlc_parser.parse_args()

    if not os.path.isfile(args.excelFile):  
        print("Excel file does not exist. Specify a valid/correct Excel file.")
        sys.exit()
        
    csv_file = kSDLC_TEMP_CSV
    print("Reading Excel file:")
    for _ in tqdm(range(100)):  
        time.sleep(0.1)
    df_excel = pd.read_excel(args.excelFile)
    
    print("Converting Excel file to CSV:")
    df_excel.to_csv(csv_file, index=False)
    for _ in tqdm(range(100)):  
        time.sleep(0.1)

    create_and_upload_gbq_table(args.excelFile, args.tableName)

    print("Script execution time:", time.time() - starttime, "seconds")
