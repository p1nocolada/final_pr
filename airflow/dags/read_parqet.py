from airflow import DAG
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

POSTGRES_URL = "jdbc:postgresql://postgres:5432/airflow"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"
POSTGRES_DRIVER = "org.postgresql.Driver"
DATA_PATH = "/opt/airflow/dags/data"
POSTGRES_CONN_ID = "postgres_default"

def write_df(df, table):
    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", table) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", POSTGRES_DRIVER) \
        .mode("append") \
        .save()    

def read_parquet():

    spark = SparkSession.builder \
        .appName("read_parquet") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()
    
    df = spark.read.parquet(DATA_PATH)

    users_df = df.select("user_id", F.col("user_phone").alias("phone")) \
                .dropDuplicates(["user_id"])
    write_df(users_df, "dwh.users_tmp")

    stores_df = df.select("store_id", F.col("store_address").alias("address")) \
                .dropDuplicates(["store_id"])
    write_df(stores_df, "dwh.stores_tmp")

    drivers_df = df.select("driver_id", F.col("driver_phone").alias("phone")) \
                .dropDuplicates(["driver_id"])
    write_df(drivers_df, "dwh.drivers_tmp")

    items_df = df.select("item_id", F.col("item_title").alias("title"), "item_category") \
                .dropDuplicates(["item_id"])
    write_df(items_df, "dwh.items_tmp")

    orders_df = df.select(
        "order_id",
        "user_id",
        "store_id",
        "driver_id",
        F.col("address_text").alias("address"),
        "created_at",
        "paid_at",
        "delivery_started_at",
        "delivered_at",
        "canceled_at",
        "payment_type",
        "delivery_cost",
        "order_discount",
        "order_cancellation_reason"
    ).dropDuplicates(["order_id"])
    write_df(orders_df, "dwh.orders_tmp")

    '''
    order_items_df = df.groupBy("order_id", "item_id").agg(
        F.sum("item_quantity").alias("quantity"),
        F.sum("item_price").alias("price"),
        F.sum("item_canceled_quantity").alias("canceled_quantity"),
        F.first("item_replaced_id"),
        F.sum("item_discount")
    )
    '''

    order_items_df = df.select(
    "order_id",
    "item_id",
    F.col("item_quantity").alias("quantity"),
    F.col("item_price").alias("price"),
    F.col("item_canceled_quantity").alias("canceled_quantity"),
    F.col("item_replaced_id"),
    F.col("item_discount")
    )

    write_df(order_items_df, "dwh.order_items_tmp")
    

    spark.stop()


with DAG(
    dag_id="read_parquet_dag",
    start_date=datetime(2026, 3, 22),
    schedule_interval="@hourly",
    catchup=False,
    tags=["spark", "parquet"]
) as dag:

    write_to_tmp_task = PythonOperator(
        task_id="read_to_tmp_task",
        python_callable=read_parquet
    )

    users_upsert_task = PostgresOperator(
        task_id="users_upsert_task",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        INSERT INTO dwh.users
        SELECT * FROM dwh.users_tmp
        ON CONFLICT (user_id)
        DO UPDATE SET phone = EXCLUDED.phone;
        """
    )
    
    stores_upsert_task = PostgresOperator(
        task_id="stores_upsert_task",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        INSERT INTO dwh.stores
        SELECT * FROM dwh.stores_tmp
        ON CONFLICT (store_id)
        DO UPDATE SET address = EXCLUDED.address;
        """
    )

    drivers_upsert_task = PostgresOperator(
        task_id="drivers_upsert_task",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        INSERT INTO dwh.drivers
        SELECT * FROM dwh.drivers_tmp
        ON CONFLICT (driver_id)
        DO UPDATE SET phone = EXCLUDED.phone;
        """
    )

    items_upsert_task = PostgresOperator(
        task_id="items_upsert_task",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        INSERT INTO dwh.items 
        SELECT * FROM dwh.items_tmp
        ON CONFLICT (item_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            item_category = EXCLUDED.item_category;
        """
    )

    orders_upsert_task = PostgresOperator(
        task_id="orders_upsert_task",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        INSERT INTO dwh.orders
        SELECT * FROM dwh.orders_tmp
        ON CONFLICT (order_id)
        DO UPDATE SET
            user_id = EXCLUDED.user_id,
            store_id = EXCLUDED.store_id,
            driver_id = EXCLUDED.driver_id,
            address = EXCLUDED.address,
            created_at = EXCLUDED.created_at;
        """
    )
    
    order_items_upsert_task = PostgresOperator(
        task_id="order_items_upsert_task",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        TRUNCATE dwh.order_items;
        INSERT INTO dwh.order_items (
            order_id, item_id, quantity, price, canceled_quantity, item_replaced_id, item_discount
        )
        SELECT * FROM dwh.order_items_tmp;
        """
    )
    
    cleanup_temp_task = PostgresOperator(
        task_id="cleanup_temp_task",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        TRUNCATE
            dwh.users_tmp,
            dwh.stores_tmp,
            dwh.drivers_tmp,
            dwh.items_tmp,
            dwh.orders_tmp,
            dwh.order_items_tmp;
        """
    ) 

    
write_to_tmp_task >> users_upsert_task >> stores_upsert_task >> drivers_upsert_task \
>> items_upsert_task >> orders_upsert_task >> order_items_upsert_task >> cleanup_temp_task 
