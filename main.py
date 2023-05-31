# CSV File validation
# This script will validate the CSV file against the schema
# and will generate a report with the errors found

import sys
import json
from typing import Optional
import pandas as pd
import re
import configparser as cp
from datetime import datetime


# Function to read the schema file
def read_schema_file(json_file: str) -> dict:
    try:
        with open(json_file) as schema_file:
            schema = json.load(schema_file)
        return schema
    except:
        print("Error: Unable to read schema file")
        sys.exit(1)


# Function to read the CSV file
def read_csv_file(csv_file: str) -> pd.DataFrame:
    try:
        csv_data = pd.read_csv(csv_file)
        return csv_data
    except:
        print("Error: Unable to read CSV file")
        sys.exit(1)


def read_parquet_file(parquet_file: str) -> pd.DataFrame:
    try:
        parquet_data = pd.read_parquet(parquet_file)
        return parquet_data
    except:
        print("Error: Unable to read Parquet file")
        sys.exit(1)


def join_dataframes(
    df1: pd.DataFrame, df2: pd.DataFrame, join: str, join_columns: list
) -> pd.DataFrame:
    """
    Function to join two dataframes
    - Args:
        - `df1`: first dataframe
        - `df2`: second dataframe
        - `join`: type of join to perform
        - `join_columns`: columns to join
    - Returns: the joined dataframe
    """
    df1 = df1[join_columns]
    df2 = df2[join_columns]
    df = pd.merge(df1, df2, how=join, on=join_columns)
    return df


# Mainly used to cross validate the data from the CSV file with the data from the database
# It will perform a left anti join between the two dataframes and will return the rows that are not present in the database
def cross_validate_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, join_columns: list):
    """
    Function to cross validate two dataframes.
    - Args:
        - `df1`: first dataframe
        - `df2`: second dataframe
        - `join_columns`: columns to join
    - Returns: the cross validated dataframe
    """

    df = join_dataframes(
        df1,
        df2,
        join="leftanti",
        join_columns=join_columns,
    )
    return df


def validate_sum(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    sum_column1: str,
    sum_column2: str,
    where1: Optional[str] = None,
    where2: Optional[str] = None,
) -> bool:
    """
    Function to validate the sum of a column in two dataframes
    - Args:
        - `df1`: first dataframe
        - `df2`: second dataframe
        - `sum_column1`: column to sum in the first dataframe
        - `sum_column2`: column to sum in the second dataframe
        - `where1`: where clause for the first dataframe
        - `where2`: where clause for the second dataframe
    - Returns:
        - `True` if the sum of the column is the same in both dataframes
        - `False` otherwise
    """

    if where1 is not None:
        df1 = df1.query(where1)
    if where2 is not None:
        df2 = df2.query(where2)
    sum1 = df1[sum_column1].sum()
    sum2 = df2[sum_column2].sum()

    print(f"Sum of {sum_column1} in df1: {sum1}")
    print(f"Sum of {sum_column2} in df2: {sum2}")

    return sum1 == sum2


def validate_count(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    count_column1: str,
    count_column2: str,
    where1: Optional[str] = None,
    where2: Optional[str] = None,
) -> bool:
    """
    Function to validate the count of a column in two dataframes
    - Args:
        - `df1`: first dataframe
        - `df2`: second dataframe
        - `count_column1`: column to count in the first dataframe
        - `count_column2`: column to count in the second dataframe
        - `where1`: where clause for the first dataframe
        - `where2`: where clause for the second dataframe
    - Returns:
        - `True` if the count of the column is the same in both dataframes
        - `False` otherwise
    """
    if where1 is not None:
        df1 = df1.query(where1)
    if where2 is not None:
        df2 = df2.query(where2)
    count1 = df1[count_column1].count()
    count2 = df2[count_column2].count()

    print(f"Count of {count_column1} in df1: {count1}")
    print(f"Count of {count_column2} in df2: {count2}")

    return count1 == count2


# Function to validate a data type
# - data_type: data type to validate
# - value: value to validate
# - options: options to validate the data type
#   - options["required"]: True if the value is required, False otherwise
#   - options["min_length"]: Minimum length of the value
#   - options["max_length"]: Maximum length of the value
#   - options["min_value"]: Minimum value of the value
#   - options["max_value"]: Maximum value of the value
#   - options["allowed_values"]: List of allowed values
#   - options["regex"]: Regular expression to match the value
#   - options["format"]: Format of the date
#   - options["precision"]: Precision of the value
# - return: True if the value is valid, False otherwise
# - Data types:
#   - string
#   - integer
#   - float
#   - boolean
#   - datetime
#   - array
#   - object
def validate_data_type(data_type: str, value: any, options):
    match data_type:
        case "string":
            return validate_string(value, options)
        case "integer":
            return validate_integer(value, options)
        case "float":
            return validate_float(value, options)
        case "boolean":
            return validate_boolean(value, options)
        case "datetime":
            return validate_datetime(value, options)
        case "array":
            return validate_array(value, options)
        case "object":
            return validate_object(value, options)
        case _:
            print(f"Error: Unknown data type: {data_type}")
            sys.exit(1)


def validate_string(value, options):
    """
    Function to validate a string
    - Args:
        - `value`: value to validate
        - `options`: options to validate the data type
            - `options["required"]`: `True` if the value is required, `False` otherwise
            - `options["min_value"]`: Minimum value of the value
            - `options["max_value"]`: Maximum value of the value
            - `options["allowed_values"]`: List of allowed values
            - `options["regex"]`: Regular expression to match the value
    - Returns:
        - `True` if the value is valid
        - `False` otherwise
    """

    if (options["required"] == True and value == "") or (
        options["required"] == True and value is None
    ):
        return False
    if options["min_length"] is not None and len(value) < options["min_length"]:
        return False
    if options["max_length"] is not None and len(value) > options["max_length"]:
        return False
    if options["allowed_values"] is not None and value not in options["allowed_values"]:
        return False
    if options["regex"] is not None and not re.match(options["regex"], value):
        return False
    return True


def validate_integer(value, options):
    """
    Function to validate an integer
    - Args:
        - `value`: value to validate
        - `options`: options to validate the data type
            - `options["required"]`: `True` if the value is required, `False` otherwise
            - `options["min_value"]`: Minimum value of the value
            - `options["max_value"]`: Maximum value of the value
    - Returns:
        - `True` if the value is valid
        - `False` otherwise
    """

    if (options["required"] == True and value == "") or (
        options["required"] == True and value is None
    ):
        return False
    try:
        value = int(value)
    except ValueError:
        return False
    if options["min_value"] is not None and value < options["min_value"]:
        return False
    if options["max_value"] is not None and value > options["max_value"]:
        return False
    return True


def validate_float(value, options):
    """
    Function to validate a boolean
    - Args:
        - `value`: value to validate
        - `options`: options to validate the data type
            - `options["required"]`: `True` if the value is required, `False` otherwise
            - `options["min_value"]`: Minimum value of the value
            - `options["max_value"]`: Maximum value of the value
            - `options["precision"]`: Precision of the value
    - Returns:
        - `True` if the value is valid
        - `False` otherwise
    """

    if (options["required"] == True and value == "") or (
        options["required"] == True and value is None
    ):
        return False
    try:
        value = float(value)
    except ValueError:
        return False
    if options["min_value"] is not None and value < options["min_value"]:
        return False
    if options["max_value"] is not None and value > options["max_value"]:
        return False
    if (
        options["precision"] is not None
        and len(str(value).split(".")[1]) > options["precision"]
    ):
        return False
    return True


def validate_boolean(value, options):
    """
    Function to validate a boolean
    - Args:
        - `value`: value to validate
        - `options`: options to validate the data type
            - `options["required"]`: `True` if the value is required, `False` otherwise
            - `options["allowed_values"]`: List of allowed values (`True`, `False`)
    - Returns:
        - `True` if the value is valid
        - `False` otherwise
    """

    allowed_values = [x == "True" for x in options["allowed_values"]]
    if (options["required"] == True and value == "") or (
        options["required"] == True and value is None
    ):
        return False
    if len(allowed_values) > 0 and value not in allowed_values:
        return False
    return True


def validate_datetime(value, options):
    """
    Function to validate a date, time or datetime
    - Args:
        - `value`: value to validate
        - `options`: options to validate the data type
            - `options["required"]`: True if the value is required, False otherwise
            - `options["min_value"]`: Minimum value of the value
            - `options["max_value"]`: Maximum value of the value
            - `options["format"]`: Format of the date
    - Returns:
        - `True` if the value is valid
        - `False` otherwise
    """

    if (options["required"] == True and value == "") or (
        options["required"] == True and value is None
    ):
        return False
    min_value = None
    max_value = None
    if options["min_value"] is not None:
        min_value = datetime.strptime(options["min_value"], options["format"])
    if options["max_value"] is not None:
        max_value = datetime.strptime(options["max_value"], options["format"])
    try:
        value = datetime.strptime(value, options["format"])
    except ValueError:
        return False
    if min_value is not None and value < min_value:
        return False
    if max_value is not None and value > max_value:
        return False
    return True


def validate_array(value, options):
    """
    Function to validate an array
    - Args:
        - `value`: value to validate
        - `options`: options to validate the data type
            - `options["required"]`: True if the value is required, False otherwise
            - `options["min_length"]`: Minimum length of the value
            - `options["max_length"]`: Maximum length of the value
            - `options["allowed_values"]`: List of allowed values
    - Returns:
        - `True` if the value is valid
        - `False` otherwise
    """
    if (options["required"] == True and value == "") or (
        options["required"] == True and value is None
    ):
        return False
    array_values = value.split(",")
    if options["min_length"] is not None and len(array_values) < options["min_length"]:
        return False
    if options["max_length"] is not None and len(array_values) > options["max_length"]:
        return False
    if options["allowed_values"] is not None:
        for array_value in array_values:
            if array_value not in options["allowed_values"]:
                return False
    return True


def validate_object(value, options):
    """Function to validate an object

    - Args:
        - `value`: value to validate
        - `options`: options to validate the data type
            - `options["required"]`: True if the value is required, False otherwise
    - Returns:
        - `True` if the value is valid
        - `False` otherwise
    """
    if (options["required"] == True and value == "") or (
        options["required"] == True and value is None
    ):
        return False
    else:
        return True


def check_required(column: dict, row: dict, index: int, errors: list) -> list:
    """Check that required fields are present.

    Checks that the required fields in the column are present in the row.

    - Args:
        - `column` (dict): The column schema.
        - `row` (dict): The row.
        - `index` (int): The index of the row in the file.
        - `errors` (list): The list of errors to append to.
    - Returns:
        - `list`: The list of errors.
    """
    if column["options"]["required"] == True and row[column["name"]] == "":
        errors.append(
            {
                "row": index,
                "column": column["name"],
                "error": f"Error: Required column {column['name']} is empty",
            }
        )
    return errors


def check_data_type(column: dict, row: dict, index: int, errors: list) -> list:
    """
    Check that the data type of a given column matches the expected data type.

    - Args:
        - `column` (dict): The column schema.
        - `row` (dict): The row.
        - `index` (int): The index of the row in the file.
        - `errors` (list): The list of errors to append to.
    - Returns:
        - `list`: The list of errors.
    """
    if row[column["name"]] != "" and not validate_data_type(
        column["type"], row[column["name"]], column["options"]
    ):
        errors.append(
            {
                "row": index,
                "column": column["name"],
                "error": f"Error: Invalid value {row[column['name']]} for column {column['name']}",
            }
        )
    return errors


# Main function
def main():
    # Configuration
    config = cp.ConfigParser()
    config.read("config.ini")
    schema_file = config["DEFAULT"]["schema_file"]
    csv_file = config["DEFAULT"]["csv_file"]
    parquet_file = config["DEFAULT"]["parquet_file"]
    report_file = config["DEFAULT"]["report_file"]

    # Read schema file
    schema = read_schema_file(schema_file)
    csv_data = read_csv_file(csv_file)
    parquet_data = read_parquet_file(parquet_file)

    # Validate CSV file
    errors = []
    for index, row in csv_data.iterrows():
        for column in schema:
            errors = check_required(column, row, index, errors)
            errors = check_data_type(column, row, index, errors)

    # Validate sum
    date_range = "date >= '2017-01-01' AND date <= '2017-12-31'"
    if not validate_sum(
        csv_data,
        parquet_data,
        "quantity",
        "quantity",
        where1=date_range,
        where2=date_range,
    ):
        errors.append(
            {
                "row": "",
                "column": "quantity",
                "error": "Error: Sum of quantity is not the same in both dataframes",
            }
        )

    # Validate count
    if not validate_count(
        csv_data,
        parquet_data,
        "quantity",
        "quantity",
        where1=date_range,
        where2=date_range,
    ):
        errors.append(
            {
                "row": "",
                "column": "quantity",
                "error": "Error: Count of quantity is not the same in both dataframes",
            }
        )

    # Generate report
    if len(errors) > 0:
        try:
            with open(report_file, "w") as report_file:
                for error in errors:
                    report_file.write(
                        f"Row: {error['row']}\t Column: {error['column']}\t\t{error['error']}\n"
                    )
            print(f"Report generated in {report_file}")
        except Exception as e:
            print(f"Error: Unable to generate report file: {e}")
            sys.exit(1)
    else:
        print("No errors found")


if __name__ == "__main__":
    main()
