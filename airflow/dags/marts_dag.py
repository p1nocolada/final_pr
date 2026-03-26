from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

POSTGRES_URL = "jdbc:postgresql://postgres:5432/airflow"
POSTGRES_PROPERTIES = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

TABLES = ["users", "stores", "drivers", "items", "orders", "order_items"]


def get_spark():
    return SparkSession.builder \
        .appName("build_marts") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()


def copy_table(table_name):
    spark = get_spark()

    df = spark.read.jdbc(
        POSTGRES_URL,
        f"dwh.{table_name}",
        properties=POSTGRES_PROPERTIES
    )

    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", f"marts.{table_name}") \
        .option("user", POSTGRES_PROPERTIES["user"]) \
        .option("password", POSTGRES_PROPERTIES["password"]) \
        .option("driver", POSTGRES_PROPERTIES["driver"]) \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()

    spark.stop()


with DAG(
    dag_id="build_marts_dag",
    start_date=datetime(2026, 3, 22),
    schedule_interval="@daily",
    catchup=False,
    tags=["spark", "marts"]
) as dag:

    tasks = []
    for table in TABLES:
        task = PythonOperator(
            task_id=f"copy_{table}",
            python_callable=copy_table,
            op_args=[table]
        )
        tasks.append(task)

    tasks[0] >> tasks[1] >> tasks[2] >> tasks[3] >> tasks[4] >> tasks[5]
