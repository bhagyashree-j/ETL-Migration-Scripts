import time
import pandas as pd
import os
import sys
import snowflake.connector
import re
import argparse
import string
from tqdm import tqdm

kSDLC_TEMP_CSV = "snowflake_tempFile.csv"

snowflake_details = {
    "user": 'user',
    "password": 'password',
    "account": 'account',
    "database": 'database',
    "schema": 'schema',
}
snowflake_data_types = {
    "int64": "INTEGER",
    "float64": "FLOAT",
    "object": "STRING",
    "datetime64[ns]": "DATE",
    "timestamp": "TIMESTAMP",
}

def clean_csv_data(df, custom_data_types):
    for column in df.columns:
        if df[column].dtype == object:
            if column in custom_data_types and custom_data_types[column] == 'date':
                df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y-%m-%d')
            elif column in custom_data_types and custom_data_types[column] == 'timestamp':
                df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            elif column in custom_data_types and custom_data_types[column] == 'time':
                df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%H:%M:%S')
            else:
                df[column] = df[column].apply(lambda x: ' '.join(x.split()) if isinstance(x, str) else x)
    return df

def display_column_info(csv_file):
    try:
        df = pd.read_csv(csv_file)
#         df.columns = [re.sub(r'^\s+|[^A-Za-z0-9_]+|\s+$', '', col.strip().upper()) for col in df.columns]
#         df.columns = [re.sub(r'\s+', '_', col) for col in df.columns]
#         df.columns = [re.sub(r'^\s+|[^A-Za-z0-9_]+|\s+$', '', col.strip().upper()).replace(' ', '_') for col in df.columns]
#         df.columns = [re.sub(r'\s+', '_', col) if ' ' in col else col for col in df.columns]
#         df.columns = [re.sub(r'\s+', '_', col.strip()) for col in df.columns]
#         df.columns = [re.sub(r'[^A-Za-z0-9_]+', '', col.strip()).replace(' ', '_') for col in df.columns]
        df.columns = [re.sub(r'[^\w\s]', '', col.strip().upper()).replace(' ', '_') for col in df.columns]

        column_info = df.dtypes
        original_data_types = {col: column_info[col] for col in df.columns}
        print("Column Names and Data Types (Snowflake):")
        print("__________________________________________________________________________________________________________________")
        print("__________________________________________________________________________________________________________________")
        for i, (col, data_type) in enumerate(column_info.items(), 1):
            print(f"{i}. {col}: {data_type}")
        return df, original_data_types
    except Exception as e:
        print(f"Error displaying column information: {e}")
        return None, None

def modify_column_data_types(df, original_data_types):
    acceptable_data_types = ["int", "float", "string", "timestamp", "date", "time"]
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
                default_data_type = map_to_snowflake_data_type(original_data_types[col])
                data_type = input(f"Specify data type for column '{col}' (default: {default_data_type}, acceptable: [int, float, string, timestamp, date, time]): ").strip()

                if data_type in acceptable_data_types:
                    custom_data_types[col] = 'int' if data_type == 'int' else data_type
                else:
                    custom_data_types[col] = default_data_type
            else:
                print("Invalid column index. Please choose a valid index.")
        except ValueError:
            print("Invalid input. Please enter a valid column index or 'exit' to finish.")

    print("__________________________________________________________________________________________________________________")
    print("Modified Data Types:")
    for col, data_type in custom_data_types.items():
        print(f"{col}: {data_type}")

    return custom_data_types

def map_to_snowflake_data_type(pandas_data_type):
    return snowflake_data_types.get(pandas_data_type.name, "STRING")

def create_table_sql(df, table_name, custom_data_types, original_data_types):
    create_table_sql = f"CREATE OR REPLACE TABLE {table_name} ("
    
    for col in df.columns:
        if col in custom_data_types:
            data_type = custom_data_types[col]
        else:
            data_type = map_to_snowflake_data_type(original_data_types[col])
        create_table_sql += f"{col} {data_type}, "

    create_table_sql = create_table_sql[:-2]
    create_table_sql += ')'
    return create_table_sql

if __name__ == "__main__":
    starttime = time.time()

    sdlc_parser = argparse.ArgumentParser(prog='SDLCUtil')
    sdlc_parser.add_argument("--excelFile", "-excel", help="Specify Excel file to convert and upload to Snowflake")
    sdlc_parser.add_argument("--SnowflakeTable", "-table", help="Specify Snowflake table name") 
    args = sdlc_parser.parse_args()

    if not os.path.isfile(args.excelFile):
        print("Excel file does not exist. Specify a valid/correct Excel file.")
        sys.exit()

    csv_file = kSDLC_TEMP_CSV
    print("Reading Excel file:")
    for _ in tqdm(range(50)):  
        time.sleep(0.1)
    df_excel = pd.read_excel(args.excelFile)
    
    print("Converting Excel file to CSV:")
    df_excel.to_csv(csv_file, index=False)
    for _ in tqdm(range(50)):  
        time.sleep(0.1)

    df, original_data_types = display_column_info(csv_file)
    custom_data_types = modify_column_data_types(df, original_data_types)
    df = clean_csv_data(df, custom_data_types)
    create_sql = create_table_sql(df, args.SnowflakeTable, custom_data_types, original_data_types)

    connection = snowflake.connector.connect(
        user=snowflake_details["user"],
        password=snowflake_details["password"],
        account=snowflake_details["account"],
        database=snowflake_details["database"],
        schema=snowflake_details["schema"],
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(create_sql)
    except Exception as e:
        print("Error in creating Snowflake table.", e)
    finally:
        print("__________________________________________________________________________________________________________________")
        print("Done with Snowflake table creation.")

    for col, data_type in custom_data_types.items():
        if data_type == 'int':
            df[col] = df[col].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
        elif data_type == 'float':
            df[col] = df[col].apply(pd.to_numeric, errors='coerce')

    df.to_csv(kSDLC_TEMP_CSV, index=False, sep='|')

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"PUT file://{kSDLC_TEMP_CSV} @%{args.SnowflakeTable}")
            cursor.execute(f"COPY INTO {args.SnowflakeTable} "
                           f"FROM @%{args.SnowflakeTable} "
                           f"FILE_FORMAT = (TYPE = 'CSV', "
                           f"FIELD_DELIMITER = '|', " 
                           f"SKIP_HEADER = 1) PURGE=TRUE") 
    except Exception as e:
        print("Error uploading data to Snowflake table.", e)
    finally:
        connection.close()

    print("Data uploaded to Snowflake table:", args.SnowflakeTable)

    csv_records = df.shape[0]
    print(f"Number of records in the CSV file: {csv_records}")

    print("Script execution time:", time.time() - starttime, "seconds")

    
