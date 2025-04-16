import os
from datetime import datetime, timedelta
import glob

from airflow import DAG
from airflow.operators.python import PythonOperator


# Import your existing modules/classes:
# - DataProcessor, or any function from data_processor.py
# - Possibly your create_spark_session, etc.
from pyspark.sql import SparkSession
from data_processor import DataProcessor
from pyspark.sql.functions import round as spark_round, col
from dotenv import load_dotenv

# -------------------------------------------------------------------------
# Default DAG arguments
# -------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 23),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -------------------------------------------------------------------------
# Define the DAG
# -------------------------------------------------------------------------
dag = DAG(
    "process_orders_dag",  # DAG ID
    default_args=default_args,
    description="Process Orders DAG similar to main.py",
    schedule_interval=timedelta(days=1),  # Run once per day
    catchup=False,
    tags=["example"],
)


# -------------------------------------------------------------------------
# Helper Functions (Airflow tasks)
# -------------------------------------------------------------------------
def load_config(**kwargs):
    """
    Load environment/config from .env or environment variables.
    Push the config dict to XCom so subsequent tasks can retrieve it.
    """
    load_dotenv("/home/maximuschan/project-2/.env")
    config = {
        "mongodb_uri": os.getenv("MONGODB_URI"),
        "mongodb_db": os.getenv("MONGO_DB"),
        "mongodb_collection_prefix": os.getenv("MONGO_COLLECTION_PREFIX"),
        "mysql_url": os.getenv("MYSQL_URL"),
        "mysql_user": os.getenv("MYSQL_USER"),
        "mysql_password": os.getenv("MYSQL_PASSWORD"),
        "mysql_connector_jar": os.getenv("MYSQL_CONNECTOR_PATH"),
        "mysql_db": os.getenv("MYSQL_DB"),
        "customers_table": os.getenv("CUSTOMERS_TABLE"),
        "products_table": os.getenv("PRODUCTS_TABLE"),
        "output_path": os.getenv("OUTPUT_PATH", "/tmp/output"),
        "reload_inventory_daily": os.getenv("RELOAD_INVENTORY_DAILY", "false").lower()
        == "true",
        "data_path": os.getenv("DATA_PATH", "/tmp/data"),
        "mongo_start_date": os.getenv("MONGO_START_DATE"),
        "mongo_end_date": os.getenv("MONGO_END_DATE"),
    }
    kwargs["ti"].xcom_push(key="config", value=config)
    print("Config loaded and pushed to XCom.")


def load_data(**kwargs):
    """
    Main logic for loading from MySQL / MongoDB, processing orders,
    generating daily summary, and forecasting.
    """

    # Pull config from XCom
    ti = kwargs["ti"]
    config = ti.xcom_pull(key="config", task_ids="load_config")

    # 1) Create Spark Session
    spark = (
        SparkSession.builder.appName("AirflowProcessOrders")
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.jars", config["mysql_connector_jar"])
        .config("spark.mongodb.input.uri", config["mongodb_uri"])
        .getOrCreate()
    )
    print("Spark session created.")

    # 2) Initialize the DataProcessor
    data_processor = DataProcessor(spark)
    data_processor.configure(config)

    # 3) Load current inventory from MySQL
    print("Loading current inventory from MySQL...")
    data_processor.current_inventory = data_processor.load_data_from_mysql(
        config["products_table"]
    )
    data_processor.original_products_df = data_processor.load_data_from_mysql(
        config["products_table"]
    )

    # 4) Load transactions
    json_files = glob.glob(
        os.path.join("/home/maximuschan/project-2/data/dataset_32/", "*.json")
    )
    transactions_df = spark.read.json(json_files, multiLine=True)

    ## save transactions to csv
    transactions_df.data_processor.save_to_csv(
        transactions_df, config["output_path"], "transactions.csv"
    )


def process_orders_in_batches(**kwargs):

    ti = kwargs["ti"]
    config = ti.xcom_pull(key="config", task_ids="load_config")

    spark = (
        SparkSession.builder.appName("AirflowProcessOrders")
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.jars", config["mysql_connector_jar"])
        .config("spark.mongodb.input.uri", config["mongodb_uri"])
        .getOrCreate()
    )

    data_processor = DataProcessor(spark)
    data_processor.configure(config)

    transactions_df = spark.read.csv(
        os.path.join(config["output_path"], "transactions.csv"),
        header=True,
        inferSchema=True,
    )

    # 5) Process orders in batch
    orders_df, order_line_items_df, updated_inventory_df = (
        data_processor.process_orders(transactions_df)
    )

    # 6) Save outputs (CSV)
    data_processor.save_to_csv(orders_df, config["output_path"], "orders.csv")
    data_processor.save_to_csv(
        order_line_items_df, config["output_path"], "order_line_items.csv"
    )
    data_processor.save_to_csv(
        updated_inventory_df, config["output_path"], "products_updated.csv"
    )
    print("Order processing CSVs saved.")

    # 7) Generate daily summary
    daily_summary = data_processor.create_daily_summary(orders_df, order_line_items_df)
    data_processor.save_to_csv(
        daily_summary, config["output_path"], "daily_summary.csv"
    )
    print("Daily summary saved.")


def forecast_sales_and_profits(**kwargs):
    ti = kwargs["ti"]
    config = ti.xcom_pull(key="config", task_ids="load_config")

    spark = (
        SparkSession.builder.appName("AirflowProcessOrders")
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.jars", config["mysql_connector_jar"])
        .config("spark.mongodb.input.uri", config["mongodb_uri"])
        .getOrCreate()
    )

    data_processor = DataProcessor(spark)
    data_processor.configure(config)

    daily_summary = spark.read.csv(
        os.path.join(config["output_path"], "daily_summary.csv"),
        header=True,
        inferSchema=True,
    )

    # 8) Forecast sales & profits
    forecast_df = data_processor.forecast_sales_and_profits(daily_summary)
    if forecast_df is not None:
        # Round columns to 2 decimals, for example
        forecast_df = forecast_df.withColumn(
            "forecasted_sales", spark_round(col("forecasted_sales"), 2)
        ).withColumn("forecasted_profit", spark_round(col("forecasted_profit"), 2))
        data_processor.save_to_csv(
            forecast_df, config["output_path"], "sales_profit_forecast.csv"
        )
        print("Forecast CSV saved.")
    else:
        print("No forecast generated.")

    spark.stop()
    print("Spark session stopped. process_orders_in_batches complete.")


# -------------------------------------------------------------------------
# Create the Airflow Operators
# -------------------------------------------------------------------------
load_config_task = PythonOperator(
    task_id="load_config",
    python_callable=load_config,
    dag=dag,
)

process_orders_task = PythonOperator(
    task_id="process_orders_in_batches",
    python_callable=process_orders_in_batches,
    dag=dag,
)

# -------------------------------------------------------------------------
# Set Dependencies
# -------------------------------------------------------------------------
load_config_task >> process_orders_task
