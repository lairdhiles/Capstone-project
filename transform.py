# Import Libraries
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import configparser
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *

def show_percentage_missing(df):
    """Visualize missing values in a spark dataframe
    
    :param df: spark dataframe
    """
    # create a dataframe with missing values count per column
    nan_count_df = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).toPandas()
    
    # convert dataframe from wide format to long format
    nan_count_df = pd.melt(nan_count_df, var_name='cols', value_name='values')
    
    # count total records in df
    total = df.count()
    
    # now lets add % missing values column
    nan_count_df['% missing values'] = 100*nan_count_df['values']/total
    
    nan_count_df.show()

def clean_spark_passanger_flight(df):
    """Clean passanger_flight dataframe

    :param df: spark dataframe with monthly immigration data
    :return: clean dataframe
    """
    total_records = df.count()
    
    print(f'Total records in dataframe: {total_records:,}')
    
    # EDA has shown these columns to exhibit over 90% missing values, and hence we drop them
    drop_columns = ['Year', 'Month', 'Total']
    df = df.drop(*drop_columns)
    
    # drop rows where all elements are missing
    df = df.dropna(how='all')

    new_total_records = df.count()
    
    print(f'Total records after cleaning: {new_total_records:,}')
    
    return df


def clean_spark_immigration_data(df):
    """Clean immigration dataframe

    :param df: spark dataframe with monthly immigration data
    :return: clean dataframe
    """
    total_records = df.count()
    
    print(f'Total records in dataframe: {total_records:,}')
    
    # EDA has shown these columns to exhibit over 90% missing values, and hence we drop them
    drop_columns = ['occup', 'entdepu','insnum']
    df = df.drop(*drop_columns)
    
    # drop rows where all elements are missing
    df = df.dropna(how='all')

    new_total_records = df.count()
    
    print(f'Total records after cleaning: {new_total_records:,}')
    
    return df


def clean_spark_temperature_data(df):
    """Clean global temperatures dataset
    
    :param df: spark dataframe representing global temperatures
    :return: clean dataframe
    """
    total_records = df.count()
    
    print(f'Total records in dataframe: {total_records:,}')
    
    # drop rows with missing average temperature
    df = df.dropna(subset=['AverageTemperature'])

    # drop rows with missing values
    new_df = df.dropna(subset='AverageTemperatureUncertainty')
    
    total_recs_after_dropping_nas = new_df.count()
    print('Total records after dropping rows with missing values: {:,}'.format(total_records-total_recs_after_dropping_nas))
    
    # drop duplicate rows
    new_df = new_df.drop_duplicates(subset=['dt', 'City', 'Country'])
    print('Rows dropped after accounting for duplicates: {:,}'.format(total_recs_after_dropping_nas-df.count()))
    
    return new_df

def clean_spark_demographics_data(df):
    """Clean the US demographics dataset
    
    :param df: spark dataframe of US demographics dataset
    :return: clean dataframe
    """
    # drop rows with missing values
    subset_cols = [
        'Male Population',
        'Female Population',
        'Number of Veterans',
        'Foreign-born',
        'Average Household Size'
    ]
    new_df = df.dropna(subset=subset_cols)
    
    rows_dropped = df.count()-new_df.count()
    print("Rows dropped with missing values: {}".format(rows_dropped))
    
    # drop duplicate columns
    new_df2 = new_df.dropDuplicates(subset=['City', 'State', 'State Code', 'Race'])
    
    rows_dropped_with_duplicates = new_df.count()-new_df2.count()
    print(f"Rows dropped after accounting for duplicates: {rows_dropped_with_duplicates}")
    
    return new_df2