import io
import pandas as pd
import re


def has_special_characters(string: str):
    pattern = r"[^a-zA-Z0-9_\s]"
    if re.search(pattern, string):
        return True
    else:
        return False


def convert_bytes(byte_size):
    # Conversion factors
    KB = 1024
    MB = 1024 * KB

    if byte_size < KB:
        return f"{byte_size} bytes"
    elif byte_size < MB:
        kb_size = byte_size / KB
        return f"{kb_size:.2f} KB"
    else:
        mb_size = byte_size / MB
        return f"{mb_size:.2f} MB"


def make_tuple(string: str, size=None):
    return string, string + f" | Size: {convert_bytes(size)}" if size else ""


def format_df(df: pd.DataFrame):
    df.columns = df.columns.str.replace(" ", "_")
    if "Unnamed:_0" in df.columns:
        df.drop("Unnamed:_0", axis="columns", inplace=True)
    df.fillna("", inplace=True)
    return df


def get_file_df(con, bucket, table):
    response = con.get_object(Bucket=bucket, Key=table)
    data = response['Body'].read()
    if ".csv" in table:
        df = pd.read_csv(io.BytesIO(data))
    elif ".xlsx" in table:
        df = pd.read_excel(io.BytesIO(data))
    else:
        df = pd.DataFrame()
    df = format_df(df)
    return df


def checkdtype(val):
    val = str(val)
    if val == "int64":
        datatype = "int"
    elif val == "bool":
        datatype = "boolean"
    elif val == "object":
        return "varchar(255)"
    elif val == "datetime64":
        datatype = "datetime"
    elif val == "float64":
        datatype = "float"
    else:
        datatype = "varchar(255)"
    return datatype


def get_schema(df: pd.DataFrame) -> list:
    columns_list = list()

    for col in df.columns:
        column = dict()
        column["column"] = col
        column["datatype"] = checkdtype(df[col].dtype)
        columns_list.append(column)

    return columns_list
