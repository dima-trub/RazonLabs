import logging
from datetime import datetime

from google.cloud import bigquery
from google.oauth2 import service_account

class BigQueryConnector:



    def __init__(self, key_path):

        credentials = service_account.Credentials.from_service_account_file(key_path)
        self.client = bigquery.Client(credentials=credentials)
        job_config = bigquery.QueryJobConfig()
        job_config.priority = bigquery.QueryPriority.INTERACTIVE


    def write(self, df, project_id, dataset_id, table_name, add_creation_log=True):
        if add_creation_log:
            df = self.add_creation_log_to_df(df)
        df.to_gbq(destination_table=f'{dataset_id}.{table_name}',
                  project_id=project_id,
                  if_exists='append', chunksize=1000)

    def bigquery_run_query(self,project_id,dataset_id,table_name,query):
        queryJob = self.client.query(query)
        logging.info(query)
        queryJob.result()

    @staticmethod

    def add_creation_log_to_df(df):
        now = datetime.now()
        df['created_at'] = now
        df['updated_at'] = now
        return df
