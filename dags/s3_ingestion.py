from airflow import DAG
from airflow.operators.empty import EmptyOperator
from astro.files import File
from astro.sql.table import Table, Metadata
from astro import sql as aql
from datetime import datetime, timedelta

AWS_CONN_ID = "aws_default"
PG_CONN_ID = "postgres_default"

s3_file_path = "s3://dbt-raw/data/"

default_args = {
    "owner":"airflow",
    "start_date":datetime(2024,10,15),
    "email_on_failure":False,
    "email_on_retry":False,
    "retries":3,
    "retry_delay":timedelta(minutes=5)
}

with DAG(
    dag_id="ingestion_raw_data",
    default_args=default_args,
    description="Execute the extraction from AWS S3 to Postgres database.",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["s3","postgres"]
):
    customer_data = aql.load_file(
        task_id="customers",
        input_file=File(
            path = s3_file_path + "olist_customers_dataset.csv"
        ),
        output_table=Table(
            name = "tb_customer",
            conn_id = PG_CONN_ID,
            metadata = Metadata(schema = "raw_data")
        )
    )
    
    start = EmptyOperator(task_id="start")
    
    geolocation_data = aql.load_file(
        task_id="geolocation",
        input_file=File(
            path = s3_file_path + "olist_geolocation_dataset.csv"
        ),
        output_table=Table(
            name = "tb_geolocation",
            conn_id = PG_CONN_ID,
            metadata = Metadata(schema = "raw_data")
        )
    )
    
    order_items_data = aql.load_file(
        task_id="order_items",
        input_file=File(
            path = s3_file_path + "olist_order_items_dataset.csv"
        ),
        output_table=Table(
            name = "tb_order_items",
            conn_id = PG_CONN_ID,
            metadata = Metadata(schema = "raw_data")
        )
    )
    
    order_payments_data = aql.load_file(
        task_id="order_payments",
        input_file=File(
            path = s3_file_path + "olist_order_payments_dataset.csv"
        ),
        output_table=Table(
            name = "tb_order_payments",
            conn_id = PG_CONN_ID,
            metadata = Metadata(schema = "raw_data")
        )
    )
    
    order_reviews_data = aql.load_file(
        task_id="order_reviews",
        input_file=File(
            path = s3_file_path + "olist_order_reviews_dataset.csv"
        ),
        output_table=Table(
            name = "tb_order_reviews",
            conn_id = PG_CONN_ID,
            metadata = Metadata(schema = "raw_data")
        )
    )
    
    orders_data = aql.load_file(
        task_id="orders",
        input_file=File(
            path = s3_file_path + "olist_orders_dataset.csv"
        ),
        output_table=Table(
            name = "tb_orders",
            conn_id = PG_CONN_ID,
            metadata = Metadata(schema = "raw_data")
        )
    )
    
    products_data = aql.load_file(
        task_id="products",
        input_file=File(
            path = s3_file_path + "olist_products_dataset.csv"
        ),
        output_table=Table(
            name = "tb_products",
            conn_id = PG_CONN_ID,
            metadata = Metadata(schema = "raw_data")
        ),
        if_exists="append"
    )
    
    sellers_data = aql.load_file(
        task_id="sellers",
        input_file=File(
            path = s3_file_path + "olist_sellers_dataset.csv"
        ),
        output_table=Table(
            name = "tb_sellers",
            conn_id = PG_CONN_ID,
            metadata = Metadata(schema = "raw_data")
        )
    )
    
    end = EmptyOperator(task_id="end")
    
    start >> [customer_data, geolocation_data, order_items_data, order_payments_data, order_reviews_data, orders_data, products_data, sellers_data] >> end

