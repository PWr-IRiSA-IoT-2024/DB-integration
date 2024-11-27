import os
import sys
import logging
from influxdb import InfluxDBClient
from dotenv import load_dotenv


load_dotenv()

required_vars = ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASS", "DB_NAME"]

for var in required_vars:
    if not os.getenv(var):
        sys.exit(f"Error: Environment variable {var} is not set.")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")


client = InfluxDBClient(DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def read_10_min_of_data_from_db(measurement):
    """
    Reads the last 10 minutes of a given measurement data points from InfluxDB database.
    
    Parameters:
        measurement (str): Name of a measurement to query.

        Example usage:
            - measurement = 'temperature'
    """

    query = f"select * from {measurement} where time > now() - 10m and time_precision='ms'"

    try:
        result = client.query(query)
        logging.info("Reading data from InfluxDB completed successfully")

        return result
    except Exception as e:
        logging.error("Failed to read data from InfluxDB: %s", e)


def read_data_from_db(query):
    """
    Reads data points from InfluxDB database.
    
    Parameters:
        query (str): InfluxDB SQL query to execute.

        Example usage:
            - query = 'select * from temperature'
    """

    try:
        result = client.query(query)
        logging.info("Reading data from InfluxDB completed successfully")

        return result
    except Exception as e:
        logging.error("Failed to read data from InfluxDB: %s", e)
    
