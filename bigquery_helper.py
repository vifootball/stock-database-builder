import os
import pandas as pd
from google.cloud import bigquery
import config
from utils import *
from bigquery_schema import Schema
from table import TableHandler
# import table_config

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './between-buy-and-sell-749f539118c3.json' # git ignore

bq_project_id = ""
bq_dataset_id = ""


def get_bq_client(project_id):
    bq_client = bigquery.Client(project=project_id)
    return bq_client


def get_exisiting_tables(project_id, dataset_id):
    client = get_bq_client(project_id)
    tables = client.list_tables(dataset_id)
    print(f"Tables contained in '{project_id}'.'{dataset_id}':")
    for table in tables:
        print(f"    {table.project}.{table.dataset_id}.{table.table_id}")
    print()


def get_size_of_existing_tables(project_id, dataset_id):
    client = get_bq_client(project_id=bq_project_id)
    QUERY = f""" 
        select 
            *
            ,round(size_bytes / pow(10,9), 2) as size_gb
            ,round(size_bytes / pow(10,6), 2) as size_mb
        from
            {bq_dataset_id}.__TABLES__
        where 
            1 = 1
    """
    df = client.query(QUERY).to_dataframe()
    print(df)
    return df


def create_table(project_id, dataset_id, table_name):
    client = get_bq_client(project_id=project_id)
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    client.create_table(table_id, exists_ok=True)


def delete_table(project_id, dataset_id, table_name):
    client = get_bq_client(project_id=project_id)
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    client.delete_table(table_id, not_found_ok=True)


def copy_local_csv_file_to_bq_table(local_fpath, bq_project_id, bq_dataset_id, bq_table_name, schema):
    client = get_bq_client(project_id=bq_project_id)
    table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_name}"
    client.create_table(table_id, exists_ok=True)

    csv_file = pd.read_csv(local_fpath)
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition = 'WRITE_TRUNCATE')
    job = client.load_table_from_dataframe(csv_file, table_id, job_config=job_config)    
    job.result()  # Wait for the job to complete.
    table = client.get_table(table_id)  # Make an API request.
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
    print()


def copy_local_csv_files_to_bq_table(local_dirpath, bq_project_id, bq_dataset_id, bq_table_name, schema):
    client = get_bq_client(project_id=bq_project_id)
    table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_name}"
    client.create_table(table_id, exists_ok=True)

    csv_file_generator = (pd.read_csv(os.path.join(local_dirpath, x)) for x in os.listdir(local_dirpath) if x.endswith('csv'))
    for i, csv_file in enumerate(csv_file_generator):
        get_write_disposition = lambda i: 'WRITE_TRUNCATE' if i==0 else 'WRITE_APPEND'                            
        print(f'Row, Col:{csv_file.shape} | Write-Disposition: {get_write_disposition(i)}')
        
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition = get_write_disposition(i))
        job = client.load_table_from_dataframe(csv_file, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete.
        table = client.get_table(table_id)  # Make an API request.
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
        print()


if __name__ == '__main__':
    bq_project_id = "between-buy-and-sell"
    bq_dataset_id ="stock"
    schema = Schema()
    
    print()
    # get_exisiting_tables(project_id=bq_project_id, dataset_id=bq_dataset_id)
    get_size_of_existing_tables(project_id=bq_project_id, dataset_id=bq_dataset_id) 

    # # MASTER_DATE
    # print()
    # copy_local_csv_file_to_bq_table(
    #     local_fpath='./downloads/bigquery_tables/master_date.csv',
    #     bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id, bq_table_name="master_date",
    #     schema=schema.MASTER_DATE
    # )

    # # MASTER_SYMBOL
    # master_symbols = concat_csv_files_in_dir(get_dirpath='./downloads/master_symbols/')
    # master_symbols.to_csv('./downloads/bigquery_tables/master_symbols.csv', index=False) # 하나씩 올리면 빈 열을 STR로 인식해야하는데 FLOAT로 인식함
    # # print(master_symbols.info())
    # print()
    # copy_local_csv_file_to_bq_table(
    #     local_fpath='./downloads/bigquery_tables/master_symbols.csv',
    #     bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id, bq_table_name='master_symbol',
    #     schema=schema.MASTER_SYMBOL
    # )

    # # HOLDINGS
    # copy_local_csv_file_to_bq_table(
    #     local_fpath='./downloads/bigquery_tables/holdings.csv',
    #     bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id, bq_table_name='holdings',
    #     schema=schema.HOLDINGS
    # )

    # # DM_GRADE
    # copy_local_csv_file_to_bq_table(
    #     local_fpath='./downloads/bigquery_tables/summary_grades.csv',
    #     bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id, bq_table_name='dm_grade',
    #     schema=schema.DM_GRADE
    # )

    # # DM_GRADE_PIVOT
    # copy_local_csv_file_to_bq_table(
    #     local_fpath='./downloads/bigquery_tables/summary_grades_piv.csv',
    #     bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id, bq_table_name='dm_grade_pivot',
    #     schema=schema.DM_GRADE_PIVOT
    # )

    # DM_CORR
    # copy_local_csv_files_to_bq_table(
    #     local_dirpath='./downloads/bigquery_tables/summary_corr_chunks/',
    #     bq_project_id=bq_project_id, bq_dataset_id=bq_dataset_id, bq_table_name='dm_correlation',
    #     schema=schema.DM_CORRELATION
    # )


    # HISTORY




