# Candy Store Data Processing & Forecasting

## 📌 Project Overview
This project processes sales transactions from a candy store dataset, extracts relevant order details, updates inventory, and generates daily summaries. Additionally, it forecasts future sales and profits using a time-series model.

The project uses **Apache Airflow** for workflow automation and **Apache Spark** for data processing. Data is sourced from **MySQL** (inventory & customers) and **MongoDB** (transactions).

## 📂 Dataset Description
The dataset contains:
- **Customers** (`customers.csv`) - Customer details from MySQL
- **Products** (`products.csv`) - Inventory data from MySQL
- **Transactions** (`transactions_YYYYMMDD.json`) - Sales transactions from MongoDB

Each transaction includes multiple items, with timestamps for order processing.

## 📦 Required Dependencies
Ensure the following packages are installed before running the project:

```sh
pip install pyspark pandas numpy prophet apache-airflow pymysql python-dotenv pymongo
```

Additional dependencies for Airflow DAG execution:

```sh
pip install apache-airflow-providers-mysql apache-airflow-providers-mongo
```

## ⚙️ Configuration

Set up the .env file with the following variables: 

```sh
DATASET_NUMBER='32'

MYSQL_CONNECTOR_PATH='/path/to/mysql-connector.jar'
MONGODB_URI='mongodb://localhost:27017'
MYSQL_URL="jdbc:mysql://localhost:3306/candy_store_${DATASET_NUMBER}"
MYSQL_USER='root'
MYSQL_PASSWORD='yourpassword'

MYSQL_DB="candy_store_${DATASET_NUMBER}"
CUSTOMERS_TABLE='customers'
PRODUCTS_TABLE='products'
MONGO_DB="candy_store_${DATASET_NUMBER}"
MONGO_COLLECTION_PREFIX='transactions_'
MONGO_START_DATE='20240201'
MONGO_END_DATE='20240210'

OUTPUT_PATH="data/output"
```

## 🚀 Running the Project

1. Start MySQL and MongoDB services

```sh
sudo systemctl start mysql
sudo systemctl start mongod
``` 

2. Run the processing pipeline

```sh
python3 src/main.py
``` 

### Expected Outputs

Processed orders `(orders.csv)`
Order line items `(order_line_items.csv)`
Updated inventory `(products_updated.csv)`
Daily sales summary `(daily_summary.csv)`
Forecast results `(sales_profit_forecast.csv)`

## 📢 Airflow DAG Usage

This project includes an Airflow DAG `(project2_dag.py)` that automates order processing.

1. Setup Airflow environment

```sh
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create --username admin --password admin --role Admin --email admin@example.com --firstname Airflow --lastname Admin
airflow webserver --port 8080
airflow scheduler
```

2. Copy the DAG file to the Airflow DAGs folder

```sh
cp project2_dag.py ~/airflow/dags
```

3. Access the Airflow UI at `http://localhost:8080` and enable the `project2_dag` DAG

4. Trigger the DAG to start the workflow

## ✅ Conclusion
This project automates order processing and forecasting using PySpark and Airflow. Future improvements could include real-time data streaming and a more advanced forecasting model.