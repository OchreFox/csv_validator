# CSV File validation
# This script will validate the CSV file against the schema
# and will generate a report with the errors found

import sys
import json
import csv
import pandas as pd
import numpy as np
import re
import configparser as cp
from datetime import datetime


# Function to read the schema file
def read_schema_file(json_file):
    try:
        with open(json_file) as schema_file:
            schema = json.load(schema_file)
        return schema
    except:
        print("Error: Unable to read schema file")
        sys.exit(1)


# Function to read the CSV file
def read_csv_file(csv_file):
    try:
        csv_data = pd.read_csv(csv_file)
        return csv_data
    except:
        print("Error: Unable to read CSV file")
        sys.exit(1)


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
def validate_data_type(data_type, value, options):
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
            print("Error: Unknown data type: {data_type}")
            sys.exit(1)


# Function to validate a string
# - value: value to validate
# - options: options to validate the data type
#   - options["required"]: True if the value is required, False otherwise
#   - options["min_length"]: Minimum length of the value
#   - options["max_length"]: Maximum length of the value
#   - options["allowed_values"]: List of allowed values
#   - options["regex"]: Regular expression to match the value
# - return: True if the value is valid, False otherwise
def validate_string(value, options):
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


# Function to validate an integer
# - value: value to validate
# - options: options to validate the data type
#   - options["required"]: True if the value is required, False otherwise
#   - options["min_value"]: Minimum value of the value
#   - options["max_value"]: Maximum value of the value
# - return: True if the value is valid, False otherwise
def validate_integer(value, options):
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


# Function to validate a float
# - value: value to validate
# - options: options to validate the data type
#   - options["required"]: True if the value is required, False otherwise
#   - options["min_value"]: Minimum value of the value
#   - options["max_value"]: Maximum value of the value
#   - options["precision"]: Precision of the value
# - return: True if the value is valid, False otherwise
def validate_float(value, options):
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


# Function to validate a boolean
# - value: value to validate
# - options: options to validate the data type
#   - options["required"]: True if the value is required, False otherwise
#   - options["allowed_values"]: List of allowed values (True, False)
# - return: True if the value is valid, False otherwise
def validate_boolean(value, options):
    allowed_values = [x == "True" for x in options["allowed_values"]]
    if (options["required"] == True and value == "") or (
        options["required"] == True and value is None
    ):
        return False
    if len(allowed_values) > 0 and value not in allowed_values:
        return False
    return True


# Function to validate a date, time or datetime
# - value: value to validate
# - options: options to validate the data type
#   - options["required"]: True if the value is required, False otherwise
#   - options["min_value"]: Minimum value of the value
#   - options["max_value"]: Maximum value of the value
#   - options["format"]: Format of the date
# - return: True if the value is valid, False otherwise
def validate_datetime(value, options):
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


# Function to validate an array
# - value: value to validate
# - options: options to validate the data type
#   - options["required"]: True if the value is required, False otherwise
#   - options["min_length"]: Minimum length of the value
#   - options["max_length"]: Maximum length of the value
#   - options["allowed_values"]: List of allowed values
# - return: True if the value is valid, False otherwise
def validate_array(value, options):
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


# Function to validate an object
# - value: value to validate
# - options: options to validate the data type
#   - options["required"]: True if the value is required, False otherwise
# - return: True if the value is valid, False otherwise
def validate_object(value, options):
    if (options["required"] == True and value == "") or (
        options["required"] == True and value is None
    ):
        return False
    else:
        return True


# Main function
def main():
    # Configuration
    config = cp.ConfigParser()
    config.read("config.ini")
    schema_file = config["DEFAULT"]["schema_file"]
    csv_file = config["DEFAULT"]["csv_file"]
    report_file = config["DEFAULT"]["report_file"]

    # Read schema file
    schema = read_schema_file(schema_file)

    # Read CSV file
    csv_data = read_csv_file(csv_file)

    # Validate CSV file
    errors = []
    for index, row in csv_data.iterrows():
        for column in schema:
            if column["options"]["required"] == True and row[column["name"]] == "":
                errors.append(
                    {
                        "row": index,
                        "column": column["name"],
                        "error": f"Error: Required column {column['name']} is empty",
                    }
                )
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

    # Generate report
    if len(errors) > 0:
        with open(report_file, "w") as report_file:
            for error in errors:
                report_file.write(
                    f"Row: {error['row']}\t Column: {error['column']}\t\t{error['error']}\n"
                )
    else:
        print("No errors found")


if __name__ == "__main__":
    main()
