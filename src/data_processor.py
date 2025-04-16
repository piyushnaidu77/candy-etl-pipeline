from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode,
    col,
    round as spark_round,
    sum as spark_sum,
    count,
    abs as spark_abs,
    when,
    lit,
    to_date,
    coalesce,
    broadcast,
    least,
    format_number,
    countDistinct,
    format_string,
)
from pyspark.sql.window import Window
from typing import Dict, Tuple
import os
import glob
import shutil
import decimal
import numpy as np
from time_series import ProphetForecaster
from datetime import datetime, timedelta
from pyspark.sql.types import DoubleType, DecimalType, IntegerType, FloatType
from pyspark.sql import Row
from pyspark.sql.functions import sum as spark_sum, count


@staticmethod
def process_product_orders(item, inv_dict: dict):
    """
    For a single product_id, sort rows by order_datetime,
    fulfill orders if stock is sufficient, otherwise set 0.
    Return an iterable of (order_id, product_id, fulfilled, unit_price, line_total).
    """
    import math

    product_id, rows = item
    rows_sorted = sorted(rows, key=lambda r: r.order_datetime)
    available, unit_price = inv_dict.get(product_id, [0, 0.0])
    results = []

    for row in rows_sorted:
        requested = row.quantity
        # If enough stock is available, fulfill
        if available > 0 and requested <= available:
            fulfilled = requested
            available -= requested
        else:
            # Cancel the order if insufficient stock
            fulfilled = 0

        line_total = round(fulfilled * unit_price, 2)
        results.append((row.order_id, product_id, fulfilled, unit_price, line_total))

    # Update dictionary
    inv_dict[product_id] = [available, unit_price]
    return results


class DataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        # Initialize all class properties
        self.config = None
        self.current_inventory = None
        self.inventory_initialized = False
        self.original_products_df = None  # Store original products data
        self.reload_inventory_daily = False  # New flag for inventory reload
        self.order_items = None
        self.products_df = None
        self.customers_df = None
        self.transactions_df = None
        self.orders_df = None
        self.order_line_items_df = None
        self.daily_summary_df = None
        self.total_cancelled_items = 0
        self.daily_orders = []  # List to store orders DataFrames from each day
        self.daily_order_items = (
            []
        )  # List to store order_line_items DataFrames from each day

    def configure(self, config: Dict) -> None:
        """Configure the data processor with environment settings"""
        self.config = config
        self.reload_inventory_daily = config.get("reload_inventory_daily", False)
        print("\nINITIALIZING DATA SOURCES")
        print("-" * 80)
        if self.reload_inventory_daily:
            print("Daily inventory reload: ENABLED")
        else:
            print("Daily inventory reload: DISABLED")

    def finalize_processing(self) -> None:
        """Finalize processing and create summary"""
        print("\nPROCESSING COMPLETE")
        print("=" * 80)
        print(f"Total Cancelled Items: {self.total_cancelled_items}")

    def load_original_products(self, products_csv: str) -> None:
        self.original_products_df = self.spark.read.csv(
            products_csv, header=True, inferSchema=True
        )

    def load_transactions(self, transactions_path: str) -> DataFrame:
        """Load transaction data from JSON files"""
        json_files = glob.glob(os.path.join(transactions_path, "*.json"))
        transactions_df = self.spark.read.json(json_files, multiLine=True)

        return transactions_df

    def load_csv_to_mysql(self, csv_path: str, table_name: str) -> None:
        """
        Load data from CSV file to MySQL table

        Args:
            csv_path: Path to CSV file
            table_name: MySQL table name
        """
        print(f"Loading {csv_path} to MySQL table {table_name}...")

        try:
            # Read the CSV file
            df = self.spark.read.csv(csv_path, header=True, inferSchema=True)

            print(f"Loading {df.count()} records with schema:")
            df.printSchema()
            df.show(5)

            driver_class = "com.mysql.cj.jdbc.Driver"

            # Write to MySQL
            df.write.format("jdbc").option("url", self.config["mysql_url"]).option(
                "dbtable", table_name
            ).option("user", self.config["mysql_user"]).option(
                "password", self.config["mysql_password"]
            ).option(
                "driver", driver_class
            ).mode(
                "overwrite"
            ).save()

            print(
                f"✓ Successfully loaded {df.count()} records to MySQL table {table_name}"
            )

        except Exception as e:
            print(f"❌ Error loading CSV to MySQL: {str(e)}")
            raise

    def load_json_to_mongodb(self, json_path: str, collection_name: str) -> None:
        """
        Load data from JSON file to MongoDB collection

        Args:
            json_path: Path to JSON file
            collection_name: MongoDB collection name
        """
        print(f"Loading {json_path} to MongoDB collection {collection_name}...")

        try:
            # Read the JSON file
            df = self.spark.read.json(json_path, multiLine=True)

            # MongoDB connection URI for this specific collection
            mongodb_uri = f"{self.config['mongodb_uri']}/{self.config['mongodb_db']}.{collection_name}"

            # Write to MongoDB
            df.write.format("mongo").mode("overwrite").option("uri", mongodb_uri).save()

            print(f"✓ Successfully loaded {df.count()} records to {collection_name}")

        except Exception as e:
            print(f"❌ Error loading JSON to MongoDB: {str(e)}")
            raise

    def load_data_from_mysql(self, table_name: str) -> DataFrame:
        """
        Load data from MySQL table to Spark DataFrame

        Args:
            table_name: MySQL table name

        Returns:
            DataFrame with data from MySQL table
        """
        print(f"\nLoading data from MySQL table {table_name}...")

        try:
            # For MySQL 8.0 and later, use this driver class
            driver_class = "com.mysql.cj.jdbc.Driver"

            # Read from MySQL
            df = (
                self.spark.read.format("jdbc")
                .option("url", self.config["mysql_url"])
                .option("dbtable", table_name)
                .option("user", self.config["mysql_user"])
                .option("password", self.config["mysql_password"])
                .option("driver", driver_class)
                .load()
            )

            # Print data info (preview + dimensions)
            num_rows = df.count()
            num_cols = len(df.columns)
            print(
                f"\n✓ Successfully loaded {num_rows} records from MySQL table {table_name}"
            )
            print(f"Dimensions: {num_rows} rows x {num_cols} columns")
            print("\nSchema:")
            df.printSchema()
            print("\nPreview:")
            df.show(5, truncate=False)

            return df

        except Exception as e:
            print(f"❌ Error loading data from MySQL: {str(e)}")
            raise

    def load_data_from_mongodb(self, collection_name: str) -> DataFrame:
        """
        Load data from MongoDB collection to Spark DataFrame

        Args:
            collection_name: MongoDB collection name

        Returns:
            DataFrame with data from MongoDB collection
        """
        print(f"Loading data from MongoDB collection {collection_name}...")

        try:
            # MongoDB connection URI for this specific collection
            mongodb_uri = f"{self.config['mongodb_uri']}/{self.config['mongodb_db']}.{collection_name}"

            # Read from MongoDB
            df = self.spark.read.format("mongo").option("uri", mongodb_uri).load()

            print(f"✓ Successfully loaded {df.count()} records from {collection_name}")
            print("Schema:")
            df.printSchema()
            print("Preview:")
            df.show(5)

            return df

        except Exception as e:
            print(f"❌ Error loading data from MongoDB: {str(e)}")
            raise

    def initialize_databases(self) -> None:
        """
        Initialize databases by loading CSV data to MySQL and JSON data to MongoDB
        """
        print("\nINITIALIZING DATABASES")
        print("-" * 80)

        # Load CSV data to MySQL
        customers_csv = os.path.join(
            self.config.get("data_path", "data"), "dataset_32", "customers.csv"
        )
        products_csv = os.path.join(
            self.config.get("data_path", "data"), "dataset_32", "products.csv"
        )

        self.load_csv_to_mysql(customers_csv, self.config["customers_table"])
        self.load_csv_to_mysql(products_csv, self.config["products_table"])

        # Load JSON data to MongoDB
        transactions_dir = os.path.join(
            self.config.get("data_path", "data"), "dataset_32"
        )

        # Get list of transaction JSON files
        transaction_files = glob.glob(
            os.path.join(transactions_dir, "transactions_*.json")
        )

        for file_path in transaction_files:
            # Extract date from filename (assuming format: transactions_YYYYMMDD.json)
            file_name = os.path.basename(file_path)
            collection_name = os.path.splitext(file_name)[0]  # Remove .json extension

            # Load to MongoDB
            self.load_json_to_mongodb(file_path, collection_name)

        print("\n✓ Database initialization complete")

    def process_orders(
        self, transactions_df: DataFrame
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        High-level method to process daily orders and return:
            (orders_df, order_line_items_final_df, updated_inventory_df)
        """

        # 1) Prepare transactions and order_items
        transactions_df, order_items = self._prepare_transactions_df(transactions_df)

        # 2) Process each day from 2024-02-01 to 2024-02-10
        order_line_items_df = self._process_orders_by_day(transactions_df)

        # 3) Build final DataFrames for order_line_items, orders, and inventory
        order_line_items_final_df, orders_df = self._build_final_data(
            order_line_items_df, transactions_df
        )

        return orders_df, order_line_items_final_df, self.current_inventory

    def _prepare_transactions_df(
        self, transactions_df: DataFrame
    ) -> Tuple[DataFrame, DataFrame]:
        """
        - Explode transaction items
        - Filter out rows with null quantity
        - Create an order_date column
        - Return the cleaned transactions_df and an order_items DataFrame
        """
        # Explode 'items' array
        exploded_df = transactions_df.withColumn("item", explode(col("items")))
        order_items = exploded_df.select(
            col("transaction_id").alias("order_id"),
            col("timestamp").alias("order_datetime"),
            col("item.product_id").alias("product_id"),
            col("item.product_name").alias("product_name"),
            col("item.qty").alias("quantity"),
        ).filter(col("quantity").isNotNull())

        # Drop rows with null quantity
        order_items = order_items.filter(col("quantity").isNotNull())

        # Add 'order_date' to the original transactions
        transactions_df = transactions_df.withColumn(
            "order_date", to_date(col("timestamp"))
        )
        # Drop any null rows in transactions_df if needed
        transactions_df = transactions_df.na.drop()

        return transactions_df, order_items

    def _process_orders_by_day(self, transactions_df: DataFrame) -> DataFrame:

        # Initialize an empty DataFrame for all daily order line items
        order_line_items_df = None

        # Process each day from Feb 1 to Feb 10
        for day in range(1, 11):
            day_str = f"2024-02-{str(day).zfill(2)}"
            print(f"\nProcessing transactions for date: {day_str}")

            # Filter for the day's transactions
            daily_transactions = transactions_df.filter(
                col("order_date") == lit(day_str)
            )
            if daily_transactions.count() == 0:
                print(f"No transactions found for {day_str}")
                continue

            # Explode items for the day
            daily_order_line_items = daily_transactions.withColumn(
                "item", explode(col("items"))
            ).select(
                col("transaction_id").alias("order_id"),
                col("customer_id"),
                col("timestamp").alias("order_datetime"),
                col("item.product_id").alias("product_id"),
                col("item.qty").alias("quantity"),
            )

            # Remove null quantities
            daily_order_line_items = daily_order_line_items.filter(
                col("quantity").isNotNull()
            )
            daily_order_line_items = daily_order_line_items.withColumn(
                "quantity",
                when(col("quantity").isNull(), lit(0)).otherwise(
                    col("quantity").cast(IntegerType())
                ),
            )

            # Join with current inventory
            daily_order_line_items = daily_order_line_items.join(
                self.current_inventory, on="product_id", how="left"
            )

            # Cast sales_price to float, coalesce stock to 0
            daily_order_line_items = daily_order_line_items.withColumn(
                "sales_price", col("sales_price").cast(FloatType())
            ).withColumn("stock", coalesce(col("stock").cast(IntegerType()), lit(0)))

            # Drop duplicates if any
            daily_order_line_items = daily_order_line_items.dropDuplicates()

            # Build inventory dict
            inv_dict = {
                row.product_id: [row.stock, float(row.sales_price)]
                for row in self.current_inventory.select(
                    "product_id", "stock", "sales_price"
                ).collect()
            }
            print(f"Inventory dictionary for {day_str}: {inv_dict}")

            # Convert daily order items to an RDD grouped by product_id
            orders_rdd = daily_order_line_items.rdd.map(
                lambda row: (row.product_id, row)
            )
            grouped = orders_rdd.groupByKey().mapValues(list)

            # Process the orders sequentially for each product
            processed_rdd = grouped.flatMap(
                lambda item: process_product_orders(item, inv_dict)
            )

            # Convert processed RDD back to a DataFrame
            daily_processed_df = processed_rdd.map(
                lambda x: Row(
                    order_id=x[0],
                    product_id=x[1],
                    quantity=x[2],
                    unit_price=x[3],
                    line_total=x[4],
                )
            ).toDF()
            daily_processed_df = daily_processed_df.orderBy("order_id", "product_id")

            # Accumulate daily DataFrames
            if order_line_items_df is None:
                order_line_items_df = daily_processed_df
            else:
                order_line_items_df = order_line_items_df.union(daily_processed_df)

            # Update global inventory with sold quantities
            self._update_inventory_with_sold_quantities(daily_processed_df)

        # End of for-loop
        return order_line_items_df

    def _update_inventory_with_sold_quantities(
        self, daily_processed_df: DataFrame
    ) -> None:
        """
        Given a DataFrame of daily processed items,
        group by product_id, sum quantity, subtract from self.current_inventory.
        """

        daily_stock_update = daily_processed_df.groupBy("product_id").agg(
            spark_sum("quantity").alias("total_quantity_sold")
        )
        self.current_inventory = (
            self.current_inventory.join(daily_stock_update, "product_id", "outer")
            .withColumn(
                "stock",
                when(col("total_quantity_sold").isNull(), col("stock")).otherwise(
                    col("stock") - col("total_quantity_sold")
                ),
            )
            .drop("total_quantity_sold")
        )

    def _build_final_data(
        self, order_line_items_df: DataFrame, transactions_df: DataFrame
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Create the final order_line_items DataFrame with selected columns,
        and create the orders DataFrame by aggregating line totals.
        Return (order_line_items_final_df, orders_df).
        """

        # Build final order_line_items DataFrame
        order_line_items_final_df = order_line_items_df.select(
            col("order_id"),
            col("product_id"),
            col("quantity"),
            col("unit_price"),
            spark_round(col("line_total").cast(DoubleType()), 2).alias("line_total"),
        ).orderBy("order_id", "product_id")

        # Format to two decimals as strings if desired
        order_line_items_final_df = order_line_items_final_df.withColumn(
            "unit_price", format_number(col("unit_price"), 2)
        ).withColumn("line_total", format_number(col("line_total"), 2))

        # Build orders DataFrame by aggregating order_line_items
        orders_summary = order_line_items_df.groupBy("order_id").agg(
            spark_round(spark_sum("line_total"), 2).alias("total_amount"),
            count("product_id").alias("num_items"),
        )

        orders_df = (
            transactions_df.select(
                col("transaction_id").alias("order_id"),
                col("timestamp").alias("order_datetime"),
                col("customer_id"),
            )
            .distinct()
            .join(orders_summary, "order_id", "left")
            .select(
                "order_id", "order_datetime", "customer_id", "total_amount", "num_items"
            )
        )

        orders_df = orders_df.orderBy("order_id").na.drop()

        # Format total_amount to 2 decimals
        orders_df = orders_df.withColumn(
            "total_amount", format_number(col("total_amount"), 2)
        )

        return order_line_items_final_df, orders_df

    def create_daily_summary(
        self, orders_df: DataFrame, order_line_items_df: DataFrame
    ) -> DataFrame:

        print("\nGenerating daily sales summary...")

        line_items_with_cost = order_line_items_df.alias("li").join(
            self.current_inventory.select("product_id", "cost_to_make").alias("inv"),
            on="product_id",
            how="left",
        )

        line_items_with_profit = line_items_with_cost.withColumn(
            "line_profit",
            spark_round(
                col("line_total").cast("double")
                - (col("quantity") * col("cost_to_make")),
                2,
            ),
        )

        orders_with_date = orders_df.select(
            col("order_id"), to_date(col("order_datetime")).alias("date")
        ).distinct()

        # Join line_items_with_profit with the date from orders
        daily_data = line_items_with_profit.join(
            orders_with_date, on="order_id", how="left"
        )

        daily_orders = orders_with_date.groupBy("date").agg(
            countDistinct("order_id").alias("num_orders")
        )

        sales_profit_per_day = daily_data.groupBy("date").agg(
            spark_round(spark_sum("line_total").cast("double"), 2).alias("total_sales"),
            spark_round(spark_sum("line_profit").cast("double"), 2).alias(
                "total_profit"
            ),
        )

        daily_summary = daily_orders.join(
            sales_profit_per_day, on="date", how="inner"
        ).orderBy("date")

        print(f"Generated daily summary for {daily_summary.count()} days")
        daily_summary.show(truncate=False)

        daily_summary = daily_summary.select(
            "date",
            "num_orders",
            format_string("%.2f", col("total_sales")).alias("total_sales"),
            format_string("%.2f", col("total_profit")).alias("total_profit"),
        )

        return daily_summary

    def save_updated_inventory(self) -> None:
        """Save the updated inventory (products_updated) as a CSV file sorted by product_id."""
        updated_inventory = self.current_inventory.select(
            col("product_id"), col("product_name"), col("stock").alias("current_stock")
        ).orderBy("product_id")

        self.save_to_csv(
            updated_inventory, self.config["output_path"], "products_updated.csv"
        )

    def save_to_csv(self, df: DataFrame, output_path: str, file_name: str) -> None:

        os.makedirs(output_path, exist_ok=True)

        temp_path = os.path.join(output_path, "temp_csv")
        final_path = os.path.join(output_path, file_name)

        try:
            # Write DataFrame to a temporary directory with explicit header option
            df.coalesce(1).write.option("header", "true").mode("overwrite").csv(
                temp_path
            )

            # List CSV files written
            temp_files = [f for f in os.listdir(temp_path) if f.endswith(".csv")]
            if not temp_files:
                raise Exception("No CSV file was created in the temporary directory.")
            temp_file = temp_files[0]

            os.rename(os.path.join(temp_path, temp_file), final_path)
            print(f"Saved CSV to {final_path}")
        except Exception as e:
            print("Error writing CSV:", str(e))
            raise
        finally:
            if os.path.exists(temp_path):
                shutil.rmtree(temp_path)

    # ------------------------------------------------------------------------------------------------
    # Try not to change the logic of the time series forecasting model
    # DO NOT change functions with prefix _
    # ------------------------------------------------------------------------------------------------
    def forecast_sales_and_profits(
        self, daily_summary_df: DataFrame, forecast_days: int = 1
    ) -> DataFrame:
        """
        Main forecasting function that coordinates the forecasting process
        """
        try:
            # Build model
            model_data = self.build_time_series_model(daily_summary_df)

            # Calculate accuracy metrics
            metrics = self.calculate_forecast_metrics(model_data)

            # Generate forecasts
            forecast_df = self.make_forecasts(model_data, forecast_days)

            forecast_df = forecast_df.withColumn(
                "forecasted_sales", spark_round(col("forecasted_sales"), 2)
            ).withColumn("forecasted_profit", spark_round(col("forecasted_profit"), 2))

            return forecast_df

        except Exception as e:
            print(
                f"Error in forecast_sales_and_profits: {str(e)}, please check the data"
            )
            return None

    def print_inventory_levels(self) -> None:
        """Print current inventory levels for all products"""
        print("\nCURRENT INVENTORY LEVELS")
        print("-" * 40)

        inventory_data = self.current_inventory.orderBy("product_id").collect()
        for row in inventory_data:
            print(
                f"• {row['product_name']:<30} (ID: {row['product_id']:>3}): {row['current_stock']:>4} units"
            )
        print("-" * 40)

    def build_time_series_model(self, daily_summary_df: DataFrame) -> dict:
        """Build Prophet models for sales and profits"""
        print("\n" + "=" * 80)
        print("TIME SERIES MODEL CONSTRUCTION")
        print("-" * 80)

        model_data = self._prepare_time_series_data(daily_summary_df)
        return self._fit_forecasting_models(model_data)

    def calculate_forecast_metrics(self, model_data: dict) -> dict:
        """Calculate forecast accuracy metrics for both models"""
        print("\nCalculating forecast accuracy metrics...")

        # Get metrics from each model
        sales_metrics = model_data["sales_model"].get_metrics()
        profit_metrics = model_data["profit_model"].get_metrics()

        metrics = {
            "sales_mae": sales_metrics["mae"],
            "sales_mse": sales_metrics["mse"],
            "profit_mae": profit_metrics["mae"],
            "profit_mse": profit_metrics["mse"],
        }

        # Print metrics and model types
        print("\nForecast Error Metrics:")
        print(f"Sales Model Type: {sales_metrics['model_type']}")
        print(f"Sales MAE: ${metrics['sales_mae']:.2f}")
        print(f"Sales MSE: ${metrics['sales_mse']:.2f}")
        print(f"Profit Model Type: {profit_metrics['model_type']}")
        print(f"Profit MAE: ${metrics['profit_mae']:.2f}")
        print(f"Profit MSE: ${metrics['profit_mse']:.2f}")

        return metrics

    def make_forecasts(self, model_data: dict, forecast_days: int = 7) -> DataFrame:
        """Generate forecasts using Prophet models"""
        print(f"\nGenerating {forecast_days}-day forecast...")

        forecasts = self._generate_model_forecasts(model_data, forecast_days)
        forecast_dates = self._generate_forecast_dates(
            model_data["training_data"]["dates"][-1], forecast_days
        )

        return self._create_forecast_dataframe(forecast_dates, forecasts)

    def _prepare_time_series_data(self, daily_summary_df: DataFrame) -> dict:
        """Prepare data for time series modeling"""
        data = (
            daily_summary_df.select("date", "total_sales", "total_profit")
            .orderBy("date")
            .collect()
        )

        dates = np.array([row["date"] for row in data])
        sales_series = np.array([float(row["total_sales"]) for row in data])
        profit_series = np.array([float(row["total_profit"]) for row in data])

        self._print_dataset_info(dates, sales_series, profit_series)

        return {"dates": dates, "sales": sales_series, "profits": profit_series}

    def _print_dataset_info(
        self, dates: np.ndarray, sales: np.ndarray, profits: np.ndarray
    ) -> None:
        """Print time series dataset information"""
        print("Dataset Information:")
        print(f"• Time Period:          {dates[0]} to {dates[-1]}")
        print(f"• Number of Data Points: {len(dates)}")
        print(f"• Average Daily Sales:   ${np.mean(sales):.2f}")
        print(f"• Average Daily Profit:  ${np.mean(profits):.2f}")

    def _fit_forecasting_models(self, data: dict) -> dict:
        """Fit Prophet models to the prepared data"""
        print("\nFitting Models...")
        sales_forecaster = ProphetForecaster()
        profit_forecaster = ProphetForecaster()

        sales_forecaster.fit(data["sales"])
        profit_forecaster.fit(data["profits"])
        print("Model fitting completed successfully")
        print("=" * 80)

        return {
            "sales_model": sales_forecaster,
            "profit_model": profit_forecaster,
            "training_data": data,
        }

    def _generate_model_forecasts(self, model_data: dict, forecast_days: int) -> dict:
        """Generate forecasts from both models"""
        return {
            "sales": model_data["sales_model"].predict(forecast_days),
            "profits": model_data["profit_model"].predict(forecast_days),
        }

    def _generate_forecast_dates(self, last_date: datetime, forecast_days: int) -> list:
        """Generate dates for the forecast period"""
        return [last_date + timedelta(days=i + 1) for i in range(forecast_days)]

    def _create_forecast_dataframe(self, dates: list, forecasts: dict) -> DataFrame:
        """Create Spark DataFrame from forecast data"""
        forecast_rows = [
            (date, float(sales), float(profits))
            for date, sales, profits in zip(
                dates, forecasts["sales"], forecasts["profits"]
            )
        ]

        return self.spark.createDataFrame(
            forecast_rows, ["date", "forecasted_sales", "forecasted_profit"]
        )
