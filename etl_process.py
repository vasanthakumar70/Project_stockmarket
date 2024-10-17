import json
import requests
import time
import os
from dotenv import load_dotenv
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, max 
import logging 

logging.basicConfig(filename='etl_process.log' ,level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


load_dotenv()

jdbc_driver_path = "sqljdbc_12.8/enu/jars/mssql-jdbc-12.8.1.jre11.jar"

spark = SparkSession.builder \
    .appName("StockMarketETL") \
    .master("local[*]") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

logger.info("Spark session started.")



def extract(bse_companies, url, querystring, headers):
    all_data = []
    request_count = 0
    request_limit = 5

    for company in bse_companies:
        querystring["symbol"] = company
        response = requests.get(url, headers=headers, params=querystring)

        if response.status_code == 200:
            logger.info(f"company:{company} Sucessfull")
            datas = response.json()
            if "Time Series (Daily)" in datas:
                time_series_data = datas["Time Series (Daily)"]
                all_data.append((company, time_series_data)) 
        else:
            logger.warning(f"Failed to fetch data for {company}, Status Code: {response.status_code}")
        
        request_count += 1
        if request_count % request_limit == 0:
            time.sleep(65)  # Rate limiting

    if all_data:
        return all_data
    else:
        logging.warning("No records in Api")




def transform(raw_data):

    previous_date = get_previous_date()

    all_rows = []

    for company, time_series_data in raw_data:
        for date, item in time_series_data.items():
            if 1==1 :#date > previous_date:
                row = {
                    'company': company,
                    'date': date,
                    'open': float(item.get('1. open', 0)),
                    'high': float(item.get('2. high', 0)),
                    'low': float(item.get('3. low', 0)),
                    'close': float(item.get('4. close', 0)),
                    'volume': float(item.get('5. volume', 0))
                }
                all_rows.append(row)

    
    if all_rows:
        logger.info("Dataframe created by API data")
        return spark.createDataFrame(all_rows).repartition(4)
        
    else:
        logger.warning("Dataframe no created")
        return None

def get_previous_date():

    jdbc_url=connect_to_sql()
    table_name = os.getenv("table")
        
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()

    previous_date = df.agg(max(col("date")).alias("max_date")).collect()[0]["max_date"]
    return str(previous_date)

def connect_to_sql():
    server = os.getenv("server")
    database = os.getenv("database")
    user = os.getenv("db_user")
    password = os.getenv("db_password")

    if not all([server, database, user, password]):
        logger.error("parameters are missing. Check environment variables")
    else:
        jdbc_url = f"jdbc:sqlserver://{server}:1433;databaseName={database};user={user};password={password};encrypt=true;trustServerCertificate=true;"
        return jdbc_url



def load_to_sql(df):

    if df is not None:
        table_name = os.getenv("table")
        jdbc_url = connect_to_sql()

        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .mode("append") \
            .save()
        logger.info("Data written successfully to SQL Server.")
    else:
        logger.warning("No data to load.")

def process_data():

    url = "https://alpha-vantage.p.rapidapi.com/query"
    querystring = {
        "function": "TIME_SERIES_DAILY",
        "symbol": "",
        "outputsize": "compact",
        "datatype": "json"
    }
    headers = {
        "x-rapidapi-key": os.getenv("access_key"),
        "x-rapidapi-host": "alpha-vantage.p.rapidapi.com"
    }

    companies = ["INFY", "TCS", "UPL", "ITC", "AAPL", "MSFT", "BRK.B", "NVDA", "JPM", "V", "PG", "WMT", "DIS", "PFE", "KO", "CSCO", "NFLX", "INTC", "AMD", "IBM", "CRM", "QCOM", "ORCL", "BA", "LLY", "NOW", "MDT", "AMGN", "HON", "SBUX"]

    raw_data = extract(companies, url, querystring, headers)

    transformed_data = transform(raw_data)

    load_to_sql(transformed_data)

    spark.stop()
    logger.info("Processing completed")

if __name__ == "__main__":
    logger.info("Main function started")
    process_data()
