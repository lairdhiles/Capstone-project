import pandas as pd
import os
import configparser
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *


def create_immigration_fact_table(spark, df, output_data):
    """This function creates an country dimension from the immigration and global land temperatures data.

    :param spark: spark session
    :param df: spark dataframe of immigration events
    :param visa_type_df: spark dataframe of global land temperatures data.
    :param output_data: path to write dimension dataframe to
    :return: spark dataframe representing calendar dimension
    """

    # rename columns to align with data model
    df = df.withColumnRenamed('ccid', 'record_id') \
        .withColumnRenamed('i94res', 'country_residence_code') \
        .withColumnRenamed('i94addr', 'state_code')

    # get visa_type dimension
    dim_df = get_visa_type_dimension(spark, output_data)

    # create a view for visa type dimension
    dim_df.createOrReplaceTempView("visa_view")

    # create a udf to convert arrival date in SAS format to datetime object
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

    # write dimension to parquet file
    df.write.parquet(output_data + "Immigration Fact", mode="overwrite")

    return immigration_df


def create_passanger_dimension_table(df, output_data):
    """This function creates an passanger flight fact table from the passanger flights data.

    :param spark: spark session
    :param df: spark dataframe of immigration events
    :param visa_type_df: spark dataframe of passanger flight data.
    :param output_data: path to write dimension dataframe to
    :return: spark dataframe representing passanger dimension table.
    """
    
    # rename columns to align with data model
    passanger_df = df.withColumnRenamed('data_dte', 'flight_date') \
        .withColumnRenamed('fg_apt_id', 'us_airport_Code') \
        .withColumnRenamed('fg_wac', 'foreign_airport_code') \
        .withColumnRenamed('carrier', 'air_carrier_code') \
        .withColumnRenamed('type', 'flight_type')
    
  # create an id field in passanger_df
    passanger_df = passanger_df.withColumn('flight_key', monotonically_increasing_id())

    # write dimension to parquet file
    passanger_df.write.parquet(output_data + "Flight Dim", mode="overwrite")

    return passanger_df


def create_demographics_dimension_table(df, output_data):
    """This function creates a us demographics dimension table from the us cities demographics data.

    :param df: spark dataframe of us demographics survey data
    :param output_data: path to write dimension dataframe to
    :return: spark dataframe representing demographics dimension
    """
    dim_df = df.withColumnRenamed('Median Age', 'median_age') \
        .withColumnRenamed('Male Population', 'male_population') \
        .withColumnRenamed('Female Population', 'female_population') \
        .withColumnRenamed('Total Population', 'total_population') \
        .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
        .withColumnRenamed('Foreign-born', 'foreign_born') \
        .withColumnRenamed('Average Household Size', 'average_household_size') \
        .withColumnRenamed('State Code', 'state_code')
    
    # lets add an id column
    dim_df = dim_df.withColumn('id', monotonically_increasing_id())

    # write dimension to parquet file
    dim_df.write.parquet(output_data + "Demographics Dim", mode="overwrite")

    return dim_df


def create_visa_type_dimension_table(df, output_data):
    """This function creates a visa type dimension from the immigration data.

    :param df: spark dataframe of immigration events
    :param output_data: path to write dimension dataframe to
    :return: spark dataframe representing calendar dimension
    """
    # create visatype df from visatype column
    visatype_df = df.select(['visatype']).distinct()

    # add an id column
    visatype_df = visatype_df.withColumn('visa_type_key', monotonically_increasing_id())

    # write dimension to parquet file
    visatype_df.write.parquet(output_data + "visatype", mode="overwrite")

    return visatype_df


def get_visa_type_dimension(spark, output_data):
    return spark.read.parquet(output_data + "visatype")


def create_temperature_dimension_table(df, output_data):
    """This function creates a temperature dimension table from the temperature data.

    :param spark: spark session
    :param df: spark dataframe of immigration events
    :param output_data: path to write dimension dataframe to
    :return: spark dataframe representing temperature dimension table.
    """
    
    # rename columns to align with data model
    temperature_df = df.withColumnRenamed('dt', 'date') \
    
    # add an id column    
    temperature_df = temperature_df.withColumn('date', monotonically_increasing_id())

    # write dimension to parquet file
    temperature_df.write.parquet(output_data + "temperature", mode="overwrite")

    return temperature_df


def create_immigration_calendar_dimension(df, output_data):
    """This function creates an immigration calendar based on arrival date

    :param df: spark dataframe of immigration events
    :param output_data: path to write dimension dataframe to
    :return: spark dataframe representing calendar dimension
    """
    # create a udf to convert arrival date in SAS format to datetime object
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

    # create initial calendar df from arrdate column
    calendar_df = df.select(['arrdate']).withColumn("arrdate", get_datetime(df.arrdate)).distinct()

    # expand df by adding other calendar columns
    calendar_df = calendar_df.withColumn('arrival_day', dayofmonth('arrdate'))
    calendar_df = calendar_df.withColumn('arrival_week', weekofyear('arrdate'))
    calendar_df = calendar_df.withColumn('arrival_month', month('arrdate'))
    calendar_df = calendar_df.withColumn('arrival_year', year('arrdate'))
    calendar_df = calendar_df.withColumn('arrival_weekday', dayofweek('arrdate'))

    # create an id field in calendar df
    calendar_df = calendar_df.withColumn('id', monotonically_increasing_id())

    # write the calendar dimension to parquet file
    partition_columns = ['arrival_year', 'arrival_month', 'arrival_week']
    calendar_df.write.parquet(output_data + "Time Dim", partitionBy=partition_columns, mode="overwrite")

    return calendar_df


# Perform quality checks here
def quality_checks(df, table_name):
    """Count checks on fact and dimension table to ensure completeness of data.

    :param df: spark dataframe to check counts on
    :param table_name: corresponding name of table
    """
    total_count = df.count()

    if total_count == 0:
        print(f"Data quality check failed for {table_name} with zero records!")
    else:
        print(f"Data quality check passed for {table_name} with {total_count:,} records.")
    return 0