# Project_stockmarket

This project shows how to get stock market data from the Alpha Vantage API, use PySpark to process it, and then save the processed data in an SQL Database.

## Project Summary

The aim of this project is to make it simpler to gather data for many companies, making it easier to analyze and store this data in a relational database. This solution uses:

- **PySpark** for parallel data processing
- **Alpha Vantage API** for stock market data
- **SQL Server** for storing and managing structured data

The project is designed to run on a set schedule (e.g., daily) to stay up-to-date with the latest stock market data.

## Highlights

- **Data Mining**: Gets stock prices and volumes for over 30 companies using the Alpha Vantage API
- **Data Transformation**: Converts the raw JSON data into useful information, including the date, opening price, closing price, highest price, lowest price, and volume.
- **Data Loading**: Saves the processed data into the SQL Database.
- **Logging**: It includes logging process to track successes and failures.

## Project Structure

```
.
├── etl_process.py         # Main Python script
├── .env                   # Environment variables (API key, database credentials, etc.)
├── etl_process.log        # Log file 
├── requirements.txt       # Required Python packages
├── README.md              # This readme file
└── sqljdbc_12.8/          # JDBC driver for connecting PySpark to SQL Server
```

### References & Downloads

- **Alpha Vantage API**: [RapidAPI](https://rapidapi.com/alphavantage/api/alpha-vantage/playground/)
- **Apache Spark**: [Spark Downloads](https://spark.apache.org/downloads.html)
- **Java Development Kit (JDK)**: [Oracle JDK 11 Downloads](https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html)
- **Hadoop Winutils**: [Winutils GitHub Repository](https://github.com/cdarlint/winutils)


