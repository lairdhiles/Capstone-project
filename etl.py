import pandas as pd
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_unixtime, to_timestamp
from pyspark.sql.functions import col, hour, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import monotonically_increasing_id

import transform
import etl_functions

config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession.builder.appName('udacity_capstone_project')\
        .config('spark.jars.packages', 'saurfang:spark-sas7bdat:3.0.0-s_2.12')\
        .getOrCreate()
    return spark


def process_immigration_data(spark, input_data, output_data, file_name, temperature_file, mapping_file):
    """Process the immigration data input file and creates dimension table and calendar, visa_type and country dimension tables.

    Parameters:
    -----------
    spark (SparkSession): spark session instance
    input_data (string): input file path
    output_data (string): output file path
    file_name (string): immigration input file name
    mapping_file (pandas dataframe): dataframe that maps country codes to country names
    temperature_file (string): global temperatures input file name
    """
    # get the file path to the immigration data
    immigration_file = input_data + file_name

    # read immigration data file
    immigration_df = spark.read.format('com.github.saurfang.sas.spark').load(immigration_file)

    # clean immigration spark dataframe
    immigration_df = transform.clean_spark_immigration_data(immigration_df)

    # create visa_type dimension table
    visatype_df = etl_functions.create_visa_type_dimension_table(immigration_df, output_data)

    # create calendar dimension table
    calendar_df = etl_functions.create_immigration_calendar_dimension(immigration_df, output_data)

    # create immigration fact table
    fact_df = etl_functions.create_immigration_fact_table(spark, immigration_df, output_data)


def process_demographics_data(spark, input_data, output_data, file_name):
    """Process the demographics data and create the usa_demographics_dim table

    Parameters:
    -----------
    spark (SparkSession): spark session instance
    input_data (string): input file path
    output_data (string): output file path
    file_name (string): usa demographics csv file name
    """

    # load demographics data
    file = input_data + file_name
    demographics_df = spark.read.csv(file, inferSchema=True, header=True, sep=';')

    # clean demographics data
    new_demographics_df = transform.clean_spark_demographics_data(demographics_df)

    # create demographic dimension table
    df = etl_functions.create_demographics_dimension_table(new_demographics_df, output_data)


def process_passanger_data(spark, input_data, output_data, file_name):
    """Process the passanger flight data and create the usa_passanger dimension table

    Parameters:
    -----------
    spark (SparkSession): spark session instance
    input_data (string): input file path
    output_data (string): output file path
    file_name (string): passanger csv file name
    """

    # load passanger data
    file = input_data + file_name
    passanger_df = spark.read.csv(file, header=True, inferSchema=True)

    # clean passanger data
    new_passanger_df = transform.clean_spark_passanger_flight(passanger_df)

    # create passanger flight table
    new_passanger_df = etl_functions.create_passanger_dimension_table(new_passanger_df, output_data)


def process_temperature_data(spark, input_data, output_data, file_name):
    """Process the global land temperatures data and return a dataframe

    Parameters:
    -----------
    spark (SparkSession): spark session instance
    input_data (string): input file path
    output_data (string): output file path
    """
    # load data
    file = input_data + file_name
    temperature_df = spark.read.csv(file, header=True, inferSchema=True)

    # clean the temperature data
    new_temperature_df = transform.clean_spark_temperature_data(temperature_df)

    # create temperature table
    new_temperature_df = etl_functions.create_temperature_dimension_table(new_temperature_df, output_data)


def main():
    spark = create_spark_session()
    input_data = "/Users/lairdhiles/Documents/Udacity/capstone-project/raw/"
    output_data = "/Users/lairdhiles/Documents/Capstone-2/processed/"

    immigration_file_name = 'i94_apr16_sub.sas7bdat'
    temperature_file_name = 'GlobalLandTemperaturesByCity.csv'
    usa_demographics_file_name = 'us-cities-demographics.csv'
    passanger_flight_file_name = 'International_Report_Passengers.csv'
    mapping_file = input_data + "i94res.csv"

    # load the i94res to country mapping data
    mapping_file = spark.read.csv(mapping_file, header=True, inferSchema=True)

    process_immigration_data(spark, input_data, output_data, immigration_file_name, temperature_file_name, mapping_file)
    process_demographics_data(spark, input_data, output_data, usa_demographics_file_name)
    process_passanger_data(spark, input_data, output_data, passanger_flight_file_name)
    process_temperature_data(spark, input_data, output_data, temperature_file_name)


if __name__ == "__main__":
    main()