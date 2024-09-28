from logging import log
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.sql.functions as f

# Creating a Spark session
def create_spark_session():
    log.info("Initializing Spark session...")
    return SparkSession.builder.appName("practise").getOrCreate()

# Loading data from a csv file
def employees(spark):
    log.info("Loading data from csv file...")
    df = spark.read.csv('/home/urban/Projekty/etl_test/practise/data/employees.csv', header=True)
    return df

# Loading data from a json file
def employees_details(spark):
    log.info("Loading data from json file...")
    df = spark.read.option("multiline", "true").json('/home/urban/Projekty/etl_test/practise/data/employees_details.json')
    return df

# Cleaning data by removing null values in csv
def clean_employees_data(employees_df):
    cleaned_employees_df = employees_df.dropna()
    return cleaned_employees_df

# Cleaning data by removing null values in json
def clean_employees_details_data(employees_details_df):
    cleaned_employees_details_df = employees_details_df.dropna()
    return cleaned_employees_details_df

# Transforming csv data structure - adding new columns
def transform_employees_data(cleaned_employees_df):
    transformed_employees_df = cleaned_employees_df.withColumn('bonus', f.col('salary') * 0.1)
    return transformed_employees_df

# Transforming json data structure - new columns with data type change
def transform_employees_details_data(cleaned_employees_details_df):
    transformed_employees_details_df = cleaned_employees_details_df.withColumn('age', F.col('age').cast('string'))
    return transformed_employees_details_df

# Aggregating CSV data - summing bonuses and calculating average salary
def aggregate_employees_data(transformed_employees_df):
    aggregated_employees_df = transformed_employees_df.groupBy("department").agg(
        F.sum("bonus").alias("total_bonus"),
        F.avg("salary").alias("average_salary")
    )
    return aggregated_employees_df

# Aggregating JSON data - calculating average age by city
def aggregate_employees_details_data(transformed_employees_details_df):
    aggregated_employees_details_df = transformed_employees_details_df.groupBy("city").agg(
        F.avg("age").alias("average_age")
    )
    return aggregated_employees_details_df

if __name__ == "__main__":
    spark = create_spark_session()

    # Loading data
    employees_df = employees(spark)
    employees_details_df = employees_details(spark)

    # Cleaning data
    cleaned_employees_df = clean_employees_data(employees_df)
    cleaned_employees_details_df = clean_employees_details_data(employees_details_df)

    # Transforming data
    transformed_employees_df = transform_employees_data(cleaned_employees_df)
    transformed_employees_details_df = transform_employees_details_data(cleaned_employees_details_df)

    # Grouping and aggregating data
    aggregated_employees_df = aggregate_employees_data(transformed_employees_df)
    aggregated_employees_details_df = aggregate_employees_details_data(transformed_employees_details_df)

    # Displaying results
    aggregated_employees_df.show(truncate=False)
    aggregated_employees_details_df.show(truncate=False)
