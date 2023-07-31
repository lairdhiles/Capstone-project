# Project README - ETL for Immigration and Passenger Flight Data

## Introduction

This project implements an Extract, Transform, and Load (ETL) process for handling immigration and passenger flight data. The ETL pipeline extracts data from various sources, transforms it into a suitable format, and loads it into dimension and fact tables in a data warehouse. The goal is to provide a structured and organized database for analysis and reporting.

## Data Sources

The ETL process uses multiple data sources, including:

1. **Immigration Events Data**: This dataset contains information about immigration events, including arrival dates, country of residence, visa types, and more.

2. **Passenger Flight Data**: The passenger flight data contains details about flights, such as flight dates, airport codes, air carriers, and flight types.

3. **Global Land Temperatures Data**: This dataset contains historical temperature data for various countries.

4. **US Cities Demographics Data**: This dataset provides demographic information for cities in the United States, including population statistics, median age, and more.

## Tools and Technologies Used

The ETL process is implemented using the following tools and technologies:

- Python (including pandas and pyspark libraries)
- Apache Spark (for processing big data)
- Jupyter Notebook (for interactive development and testing)

## ETL Workflow

The ETL workflow consists of the following steps:

1. **Data Cleaning and Preprocessing**: Each data source is preprocessed to handle missing values, duplicates, and format inconsistencies. Data cleaning is performed for all datasets to ensure data quality.

2. **Dimension Table Creation**: Dimension tables are created to store descriptive attributes such as flight details, demographics, visa types, temperature, and time. These dimension tables are designed to provide additional context for the fact table.

3. **Fact Table Creation**: The fact table is created to store the core measures or metrics derived from the immigration and passenger flight data. It contains foreign keys to link with the dimension tables.

4. **Data Loading**: The cleaned and transformed data is loaded into the corresponding dimension and fact tables in the data warehouse. The data is stored in a structured format such as Parquet for efficient querying and analysis.

## Data Model

The data model for the ETL process consists of the following tables:

### Dimension Tables:

1. **Flight Dimension (Flight Dim)**:
   - Columns:
     - id (Primary Key): Unique identifier for each record in the dimension table.
     - flight_date: Flight date in the format 'YYYY-MM-DD'.
     - us_airport_Code: US airport code.
     - foreign_airport_code: Foreign airport code.
     - air_carrier_code: Air carrier code.
     - flight_type: Flight type (e.g., international, domestic).

2. **Demographics Dimension (Demographics Dim)**:
   - Columns:
     - id (Primary Key): Unique identifier for each record in the dimension table.
     - median_age: Median age of the city's population.
     - male_population: Number of male residents in the city.
     - female_population: Number of female residents in the city.
     - total_population: Total population of the city.
     - number_of_veterans: Number of veterans in the city.
     - foreign_born: Number of foreign-born residents in the city.
     - average_household_size: Average household size in the city.
     - state_code: Code representing the state of the city.

3. **Visa Type Dimension (visatype)**:
   - Columns:
     - visa_type_key (Primary Key): Unique identifier for each visa type.
     - visatype: Visa type category.

4. **Temperature Dimension (temperature)**:
   - Columns:
     - date: Date in the format 'YYYY-MM-DD'.

5. **Time Dimension (Time Dim)**:
   - Columns:
     - id (Primary Key): Unique identifier for each record in the dimension table.
     - arrdate: Arrival date in the format 'YYYY-MM-DD'.
     - arrival_day: Day of the month of arrival (1 to 31).
     - arrival_week: Week of the year of arrival (1 to 53).
     - arrival_month: Month of arrival (1 to 12).
     - arrival_year: Year of arrival (e.g., 2016, 2017).
     - arrival_weekday: Day of the week of arrival (1 = Sunday, 2 = Monday, ..., 7 = Saturday).

### Fact Table:

1. **Immigration Fact Table (Immigration Fact)**:
   - Columns:
     - id (Primary Key): Unique identifier for each record in the fact table.
     - record_id: Unique identifier for each immigration record.
     - country_residence_code: Code representing the country of residence.
     - state_code: Code representing the state.
     - visa_type_key (Foreign Key): References the primary key visa_type_key in the visatype dimension table.
     - arrival_date_id (Foreign Key): References the primary key id in the Time Dim dimension table.
     - demographics_id (Foreign Key): References the primary key id in the Demographics Dim dimension table.

## Running the ETL Process

To execute the ETL process and create the dimension and fact tables, follow these steps:

1. Make sure you have installed the required libraries and tools, including pandas, pyspark, and Jupyter Notebook.

2. Set up the data sources: Ensure that the required data files (immigration data, passenger flight data, global land temperatures data, and US demographics data) are available in the specified directories.

3. Open the Jupyter Notebook containing the ETL code.

4. Run each cell in the notebook to execute the ETL process step by step. This will perform data cleaning, create dimension and fact tables, and load data into the data warehouse.

5. After running the entire notebook, the dimension and fact tables will be created and stored in the specified output directories.

## Conclusion

The ETL process presented in this project helps to organize and structure immigration and passenger flight data for analysis and reporting. By creating dimension and fact tables, the data model allows for efficient querying and insightful analysis. The process can be further enhanced and extended based on specific business requirements and data sources.