from pyspark.sql import SparkSession, DataFrame
from data_processor import DataProcessor
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
from pyspark.sql.functions import col, round as spark_round
from typing import Dict, Tuple
import traceback


def create_spark_session(app_name: str = "CandyStoreAnalytics") -> SparkSession:
    """
    Create and configure Spark session with MongoDB and MySQL connectors
    """
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.mongodb.input.uri", os.getenv("MONGODB_URI"))
        .getOrCreate()
    )


def get_date_range(start_date: str, end_date: str) -> list[str]:
    """Generate a list of dates between start and end date"""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    date_list = []

    current = start
    while current <= end:
        date_list.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    return date_list


def print_header():
    print("*" * 80)
    print("                        CANDY STORE DATA PROCESSING SYSTEM")
    print("                               Analysis Pipeline")
    print("*" * 80)


def print_processing_period(date_range: list):
    print("\n" + "=" * 80)
    print("PROCESSING PERIOD")
    print("-" * 80)
    print(f"Start Date: {date_range[0]}")
    print(f"End Date:   {date_range[-1]}")
    print("=" * 80)


def setup_configuration() -> Tuple[Dict, list]:
    """Setup application configuration"""
    load_dotenv()
    config = load_config()
    date_range = get_date_range(
        os.getenv("MONGO_START_DATE"), os.getenv("MONGO_END_DATE")
    )
    return config, date_range


def load_config() -> Dict:
    """Load configuration from environment variables"""
    return {
        "mongodb_uri": os.getenv("MONGODB_URI"),
        "mongodb_db": os.getenv("MONGO_DB"),
        "mongodb_collection_prefix": os.getenv("MONGO_COLLECTION_PREFIX"),
        "mysql_url": os.getenv("MYSQL_URL"),
        "mysql_user": os.getenv("MYSQL_USER"),
        "mysql_password": os.getenv("MYSQL_PASSWORD"),
        "mysql_db": os.getenv("MYSQL_DB"),
        "customers_table": os.getenv("CUSTOMERS_TABLE"),
        "products_table": os.getenv("PRODUCTS_TABLE"),
        "output_path": os.getenv("OUTPUT_PATH"),
        "reload_inventory_daily": os.getenv("RELOAD_INVENTORY_DAILY", "false").lower()
        == "true",
    }


def initialize_data_processor(spark: SparkSession, config: Dict) -> DataProcessor:
    """Initialize and configure the DataProcessor"""
    print("\nINITIALIZING DATA SOURCES")
    print("-" * 80)

    data_processor = DataProcessor(spark)
    data_processor.config = config
    return data_processor


def print_processing_complete(total_cancelled_items: int) -> None:
    """Print processing completion message"""
    print("\nPROCESSING COMPLETE")
    print("=" * 80)
    print(f"Total Cancelled Items: {total_cancelled_items}")


def print_daily_summary(orders_df, order_items_df, cancelled_count):
    """Print summary of daily processing"""
    processed_items = order_items_df.filter(col("quantity") > 0).count()
    print("\nDAILY PROCESSING SUMMARY")
    print("-" * 40)
    print(f"• Successfully Processed Orders: {orders_df.count()}")
    print(f"• Successfully Processed Items: {processed_items}")
    print(f"• Items Cancelled (Inventory): {cancelled_count}")
    print("-" * 40)


def main():
    print_header()

    # Setup configuration and get date range (assumed to be 10 days)
    config, date_range = setup_configuration()
    print_processing_period(date_range)

    # Initialize Spark session and the DataProcessor instance
    spark = create_spark_session()
    data_processor = DataProcessor(spark)
    data_processor.configure(config)

    # Assume current_inventory is loaded once from MySQL at the start (no reload after demo)
    print("Loading products (current inventory) from MySQL...")
    data_processor.current_inventory = data_processor.load_data_from_mysql(
        config["products_table"]
    )
    data_processor.original_products_df = data_processor.load_data_from_mysql(
        config["products_table"]
    )
    data_processor.current_inventory.show(5)

    # Process each day's transactions (batch processing)
    data_processor.current_inventory = data_processor.load_data_from_mysql(
        config["products_table"]
    )
    data_processor.original_products_df = data_processor.load_data_from_mysql(
        config["products_table"]
    )

    # Load transactions for the period (assuming all transactions are in a folder)
    transactions_path = os.path.join(config.get("data_path", "data"), "dataset_32")
    transactions_df = data_processor.load_transactions(transactions_path)

    # Process orders in batch (for Feb 1 to Feb 10)
    orders_df, order_line_items_final_df, updated_inventory_df = (
        data_processor.process_orders(transactions_df)
    )

    # Save updated inventory to CSV
    data_processor.save_updated_inventory()

    # Save results as CSV files
    data_processor.save_to_csv(orders_df, config["output_path"], "orders.csv")
    data_processor.save_to_csv(
        order_line_items_final_df, config["output_path"], "order_line_items.csv"
    )

    # Generate daily summary for forecasting
    daily_summary = data_processor.create_daily_summary(
        orders_df, order_line_items_final_df
    )
    data_processor.save_to_csv(
        daily_summary, config["output_path"], "daily_summary.csv"
    )

    # Generate forecasts
    forecast_df = data_processor.forecast_sales_and_profits(daily_summary)

    # save forecast to CSV
    data_processor.save_to_csv(
        forecast_df, config["output_path"], "sales_profit_forecast.csv"
    )

    spark.stop()


if __name__ == "__main__":
    main()
