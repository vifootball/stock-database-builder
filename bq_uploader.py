from utils import *
from constants import *
from google.cloud import bigquery


class BqUploader():
    def __init__(self):
        self.bq_project_id = BQ_PROJECT_ID
        self.bq_dataset_id = BQ_DATASET_ID
        self.bq_table_id_summary = BQ_TABLE_ID_SUMMARY
        self.bq_table_id_history = BQ_TABLE_ID_HISTORY

    @measure_time
    def upload_summaries_to_bq(self):
        client = bigquery.Client(project=self.bq_project_id)
        table_id = f"{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_table_id_summary}"
        client.create_table(table_id, exists_ok=True)

        dirpath_summaries = os.path.join(DIR_DOWNLOAD, SUBDIR_SUMMARY)
        summary_generator = (pd.read_csv(os.path.join(dirpath_summaries, x)) for x in os.listdir(dirpath_summaries) if x.endswith('csv'))
        for i, summary in enumerate(summary_generator):
            get_write_disposition = lambda i: 'WRITE_TRUNCATE' if i==0 else 'WRITE_APPEND'                            
            print(f'Row, Col:{summary.shape} | Write-Disposition: {get_write_disposition(i)}')

            job_config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("asset_class", "STRING"),
                    bigquery.SchemaField("dividend", "FLOAT64"),
                    bigquery.SchemaField("dividend_paid_or_not", "FLOAT64"),
                    bigquery.SchemaField("net_assets_abbv", "STRING"),
                    bigquery.SchemaField("currency", "STRING"),
                    bigquery.SchemaField("country", "STRING"),
                    bigquery.SchemaField("fund_family", "STRING"),                    
                    bigquery.SchemaField("inception_date", "STRING"),
                    bigquery.SchemaField("isin", "STRING"),
                    bigquery.SchemaField("stock_exchange", "STRING"),
                    bigquery.SchemaField("stock_split", "FLOAT64"),
                    bigquery.SchemaField("volume", "FLOAT64"),
                    bigquery.SchemaField("volume_of_dollar", "FLOAT64"),
                    bigquery.SchemaField("volume_of_share", "FLOAT64")
                ],
                write_disposition = get_write_disposition(i)
            )
            job = client.load_table_from_dataframe(summary, table_id, job_config=job_config)
            job.result()  # Wait for the job to complete.
            table = client.get_table(table_id)  # Make an API request.
            print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
            print()

    def upload_history_to_bq(self):
        client = bigquery.Client(project=self.bq_project_id)
        table_id = f"{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_table_id_history}"
        client.create_table(table_id, exists_ok=True)

        dirpath_histories = os.path.join(DIR_DOWNLOAD, SUBDIR_HISTORY_CHUNK)
        history_generator = (pd.read_csv(os.path.join(dirpath_histories, x)) for x in tqdm(os.listdir(dirpath_histories)) if x.endswith('csv'))
        for i, summary in enumerate(history_generator):
            get_write_disposition = lambda i: 'WRITE_TRUNCATE' if (i==0) else 'WRITE_APPEND'                            
            print(f'Row, Col:{summary.shape} | Write-Disposition: {get_write_disposition(i)}')

            job_config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("open", "FLOAT64"),
                    bigquery.SchemaField("high", "FLOAT64"),
                    bigquery.SchemaField("low", "FLOAT64"),
                    bigquery.SchemaField("close", "FLOAT64"),                    
                    bigquery.SchemaField("stock_split", "FLOAT64"),
                    bigquery.SchemaField("dividend", "FLOAT64"),
                    bigquery.SchemaField("dividend_paid_or_not", "FLOAT64"),
                    bigquery.SchemaField("volume", "FLOAT64"),
                    bigquery.SchemaField("volume_of_dollar", "FLOAT64"),
                    bigquery.SchemaField("volume_of_share", "FLOAT64")
                ],
                write_disposition=get_write_disposition(i)
            )
            job = client.load_table_from_dataframe(summary, table_id, job_config=job_config)
            job.result()  # Wait for the job to complete.
            table = client.get_table(table_id)  # Make an API request.
            print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

    def check_table(self):
        pass

if __name__ == "__main__":
    if 'stock-database-builder' in os.listdir():
        os.chdir('stock-database-builder')

    bq_uploader = BqUploader()
    bq_uploader.upload_summaries_to_bq()
    bq_uploader.upload_history_to_bq()