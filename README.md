# example-pyspark-use
Example use of a PySpark transformation script on fake CSV and JSON files. Demonstrates data loading, cleaning, transformation, and aggregation, with practical operations like column calculations and data type changes, along with logging for ETL steps.
# Spark Example: Data Processing and Aggregation

This repository contains a basic example of how to use Apache Spark for loading, cleaning, transforming, and aggregating data from CSV and JSON files. The code demonstrates key functionalities of Spark's DataFrame API, including data cleaning, data transformation, and aggregation.

## Overview

This project is designed to show how to use Apache Spark for simple (Transform & Load) operations. It includes loading data from CSV and JSON files, cleaning the data, transforming the data by adding new columns, and finally performing aggregation operations on the data.

The script processes employee data:
- The CSV file contains general information about employees (such as salary).
- The JSON file contains detailed information about employees (such as their age and city).

## Technologies Used

- Python 3.5
- PySpark
- Logging (for simple logging of ETL steps)

## Requirements

To run this project, you'll need to have Python and Apache Spark installed locally.

1. Clone this repository to your local machine:

    bash:
    git clone https://github.com/ur64n/example-pyspark-use
    

2. Install the necessary Python dependencies:

    bash:
    pip install pyspark
    

3. Ensure that Spark is installed and available in your system path.

4. Add your CSV and JSON data files to the appropriate directory (in this case, `/home/urban/Projekty/etl_test/practise/data/`).

## Usage

Once everything is set up, you can run the script using the following command in apropriate path:

python3 asd.py
