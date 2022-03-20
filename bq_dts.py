# --------------------------------------------------------------------------------
# Load The Dependencies
# --------------------------------------------------------------------------------

import csv
import datetime
import io
import logging

from airflow import models
from airflow.operators import dummy
from airflow.providers.google.cloud.transfers import bigquery_to_gcs
from airflow.providers.google.cloud.transfers import gcs_to_bigquery
from airflow.providers.google.cloud.transfers import gcs_to_gcs


# --------------------------------------------------------------------------------
# Set default arguments
# --------------------------------------------------------------------------------

# If you are running Airflow in more than one time zone
# see https://airflow.apache.org/docs/apache-airflow/stable/timezone.html
# for best practices
yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'start_date': yesterday,
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# --------------------------------------------------------------------------------
# Set variables
# --------------------------------------------------------------------------------

# 'table_list_file_path': This variable will contain the location of the main
# file.
#table_list_file_path = gs://bqdts_us/*.csv

# Source Bucket
source_bucket = "bqdts_us"

# Destination Bucket
dest_bucket = "bqdts_eu"

# --------------------------------------------------------------------------------
# Set GCP logging
# --------------------------------------------------------------------------------

logger = logging.getLogger('bq_copy_us_to_eu_01')

# --------------------------------------------------------------------------------
# Functions
# --------------------------------------------------------------------------------


# --------------------------------------------------------------------------------
# Main DAG
# --------------------------------------------------------------------------------

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'composer_sample_bq_copy_across_locations',
        default_args=default_args,
        schedule_interval=None) as dag:
    start = dummy.DummyOperator(
        task_id='start',
        trigger_rule='all_success'
    )

    end = dummy.DummyOperator(
        task_id='end',
        trigger_rule='all_success'
    )

    # Get the table list from main file
    # all_records = read_table_list(table_list_file_path)

    # Loop over each record in the 'all_records' python list to build up
    # Airflow tasks
    #for record in all_records:
    #logger.info('Generating tasks to transfer table: {}'.format(record))
proj='proj'
dataset_source='source_dataset'
dataset_dest='dest_dataset'
table_source = 'T1_transfer'
table_dest = 'T1_transfer'

BQ_to_GCS = bigquery_to_gcs.BigQueryToGCSOperator(
    # Replace ":" with valid character for Airflow task
    task_id='{}_BQ_to_GCS'.format(table_source.replace(":", "_")),
    source_project_dataset_table=proj + '.' + dataset_source + '.' + table_source,
    destination_cloud_storage_uris=['{}-*.avro'.format(
        'gs://' + source_bucket + '/' + table_source)],
    export_format='AVRO',
    dag=dag
)

GCS_to_GCS = gcs_to_gcs.GCSToGCSOperator(
    # Replace ":" with valid character for Airflow task
    task_id='{}_GCS_to_GCS'.format(table_source.replace(":", "_")),
    source_bucket=source_bucket,
    source_object='{}-*.avro'.format(table_source),
    destination_bucket=dest_bucket,
    dag=dag
    # destination_object='{}-*.avro'.format(table_dest)
)

GCS_to_BQ = gcs_to_bigquery.GCSToBigQueryOperator(
    # Replace ":" with valid character for Airflow task
    task_id='{}_GCS_to_BQ'.format(table_dest.replace(":", "_")),
    bucket=dest_bucket,
    source_objects=['{}-*.avro'.format(table_source)],
    destination_project_dataset_table=proj + '.' + dataset_dest + '.' + table_dest,
    source_format='AVRO',
    write_disposition='WRITE_TRUNCATE',
    autodetect=True,
    dag=dag
)

start >> BQ_to_GCS >> GCS_to_GCS >> GCS_to_BQ >> end
