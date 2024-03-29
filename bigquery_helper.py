import os
import pandas as pd
from google.cloud import bigquery
import config
from table import TableHandler
import table_config
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './between-buy-and-sell-749f539118c3.json' # git ignore

class BigQueryHelper:
    def __init__(self):
        self.bq_project_id = config.BQ_PROJECT_ID
        self.bq_dataset_id = config.BQ_DATASET_ID
        self.client = bigquery.Client(project=self.bq_project_id)
        
    def copy_local_csv_files_to_bq_table(self, local_dirpath, bq_table_id, table_config):
        client = self.client
        table_id = f"{self.bq_project_id}.{self.bq_dataset_id}.{bq_table_id}"
        client.create_table(table_id, exists_ok=True)

        table_handler = TableHandler(table_config=table_config)
        schema = table_handler.get_bq_schema()

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

    def get_exisiting_tables(self):
        client = self.client
        tables = client.list_tables(self.bq_dataset_id)
        print("Tables contained in '{}':".format(self.bq_dataset_id))
        for table in tables:
            print(f"{table.project}.{table.dataset_id}.{table.table_id}")

if __name__ == '__main__':
    bq = BigQueryHelper()
    # metadata: dim_etf
    # bq.copy_local_csv_files_to_bq_table(
    #     local_dirpath=new_config.DIR_METADATA_CHUNK,
    #     bq_table_id=new_config.BQ_TABLE_ID_DIM_ETF,
    #     table_config=new_table_config.METADATA
    # )

    # # date_dim: dim_date
    # bq.copy_local_csv_files_to_bq_table(
    #     local_dirpath=new_config.DIR_DATEDIM,
    #     bq_table_id=new_config.BQ_TABLE_ID_DIM_DATE,
    #     table_config=new_table_config.DATE_DIM
    # )

    # history: fact_etf
    # bq.copy_local_csv_files_to_bq_table(
    #     local_dirpath=new_config.DIR_HISTORY_CHUNK,
    #     bq_table_id=new_config.BQ_TABLE_ID_FACT_ETF,
    #     table_config=new_table_config.TRG_HISTORY
    # )





