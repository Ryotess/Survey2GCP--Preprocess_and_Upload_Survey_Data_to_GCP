#--------------------------Setups---------------------------------#
# Packages
from zipfile import ZipFile
import zipfile
import pyreadstat
import pandas as pd
import numpy as np
import os
import re

# Color Settings
class bcolors:
    YELLOW = '\033[93m'
    BOLD = '\033[01m'
    GREEN = '\033[92m'
    RED = '\033[91m'
    ENDC = '\033[0m'

# Global Settings
## Input path | Surveys Raw data (.zip format)
input_path = './input/'

## output and csv folder path
output_path = './output/' # For unzipped .sav files
csv_path = './csv_file/' # For Preprocessed .csv files

## Credentials for GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./credentials/Your credential files' name" # Add credentials to environment

## Project and dataset to upload
PROJECT_ID = 'project-id' # Put your project id here
dataset_id = 'dataset-id' # dataset to upload

## create output and csv folder if not exist
if not os.path.exists(output_path):
   os.makedirs(output_path)
if not os.path.exists(csv_path):
   os.makedirs(csv_path)

#--------------------------Part A. Unzip and Preprocessing-----------------------------#
## Unzip
dir_list = os.listdir(input_path)
for d in dir_list:
    if('.zip' in d):
        zip_file = zipfile.ZipFile(input_path+d)
        file_name = d.replace("zip","sav")
        zipinfos = zip_file.infolist()
        zipinfos[0].filename = file_name
        zip_file.extract(zipinfos[0], path = output_path)

## Preprocessing/ Analytics function
def Proces_func(df, your parameters here):
    """define your processing function here"""
    return df_processed

## Preprocess and save as csv files
for file in os.listdir(output_path):
    if('.sav' in file):
        print(f"{bcolors.YELLOW}Process file: {bcolors.ENDC}",file)
        try:
            df, meta = pyreadstat.read_sav(output_path + file) # read survey raw data
            new_name = "you can use regular expression or your naming rule to create a new name" # name for output .csv
            # # extract spss variables(optional: if your analytics need these info)
            # labels_dict = meta.column_names_to_labels
            # value_labels_dict = meta.variable_value_labels
            # measure_dict = meta.variable_measure
            # variable_type = meta.original_variable_types

            # Preprocessing/Analytics function
            df_processed = Proces_func()

            # # You may want to save a new version of processed SAV file,
            # # remember if you change the variables/columns during the preprocessing function,
            # # those saving information like variable_format/column_labels... also need to be modified.
            # pyreadstat.write_sav(df_processed, f'{output_path}{file}_processed',
            #                     variable_format=variable_type ,
            #                     column_labels=labels_dict ,
            #                     variable_value_labels=value_labels_dict,
            #                     variable_measure=measure_dict,
            #                     row_compress=True)
            
            df_processed.to_csv(f'{csv_path}{new_name}.csv', index=False)
            print(f'{bcolors.BOLD}===>{bcolors.ENDC} {csv_path}{new_name}.csv :{bcolors.GREEN}Done{bcolors.ENDC}.')
            
        except:
            print(f'{bcolors.BOLD}===>{bcolors.ENDC} {csv_path}{new_name}.csv :{bcolors.RED}Failed{bcolors.ENDC}.')

        print('---')

#---------------------------Part B. Upload to GCP bigquery----------------------------#
## GCP upload function
def GCP_upload(PROJECT_ID, csv_file, dataset_id):
    ## BigQuery package
    from google.cloud import bigquery

    ## create a client instance for your project
    client = bigquery.Client(project=PROJECT_ID, location="US")

    ## Upload files in folderpath
    filelist = os.listdir(csv_file) # get all files' name in folderpath

    for file in filelist:
        if file.endswith('.csv'):
            filename = f'{csv_file}{file}' # file path
            table_id = file[:-4:] # use file name (remove .csv) as table name

            #check if the table exists
            tables = client.list_tables(dataset_id)
            table_list = [table.table_id for table in tables]
            if not table_id in table_list:
                client.create_table(f"{PROJECT_ID}.{dataset_id}.{table_id}") # create table to store data
            else:
                # upload those data not in dataset
                df = pd.read_csv(f'{csv_file}{file}')
                query = f"""
                SELECT *
                FROM `{PROJECT_ID}.{dataset_id}.{table_id}`
                """
                data_exist = client.query(query, project=PROJECT_ID).to_dataframe()
                data_exist = data_exist['Your key column name of survey that can check the uniqueness'].tolist()
                df = df.loc[~(df['Your key column name of survey that can check the uniqueness'].isin(data_exist)),]
                df.to_csv(f'{csv_file}{file}', index = False)
                if len(df.index) == 0:
                    print("Loaded 0 rows into {}: {}.".format(dataset_id, table_id))
                    continue
            
            ## tell the client everything it needs to know to upload our csv
            dataset_ref = client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.autodetect = True
            
            
            ## load the csv into bigquery
            with open(filename, "rb") as source_file:
                job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

            job.result()  # Waits for table load to complete.

            ## looks like everything worked :)
            print("Loaded {} rows into {}: {}.".format(job.output_rows, dataset_id, table_id))
    
    print("Done")

## UPLOAD
GCP_upload(PROJECT_ID, csv_path, dataset_id)