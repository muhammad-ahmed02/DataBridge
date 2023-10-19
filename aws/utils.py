import io
import pandas as pd
import re
import fastavro
from fastparquet import ParquetFile


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
    GB = 1024 * MB

    if byte_size < KB:
        return f"{byte_size} bytes"
    elif byte_size < MB:
        kb_size = byte_size / KB
        return f"{kb_size:.2f} KB"
    elif byte_size < GB:
        mb_size = byte_size / MB
        return f"{mb_size:.2f} MB"
    else:
        gb_size = byte_size / GB
        return f"{gb_size:.2f} GB"


def make_tuple(string: str, size=None):
    return string, string + f"{' | Size: '+convert_bytes(size) if size else ''}"


def check_unique_tuple(new_tuple, tuple_list):
    new_tuple_first_element = new_tuple[0]
    existing_first_elements = {t[0] for t in tuple_list}

    if new_tuple_first_element not in existing_first_elements:
        return True
    return False


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
    elif ".json" in table:
        df = pd.read_json(io.BytesIO(data))
    elif ".xml" in table:
        df = pd.read_xml(io.BytesIO(data))
    elif ".parquet" in table:
        pf = ParquetFile(data)
        df = pf.to_pandas()
    elif ".html" in table:
        df = pd.read_html(data)
    elif ".avro" in table:
        # Open the Avro file and read it as a Pandas DataFrame
        with open(data, 'rb') as avro_file:
            avro_reader = fastavro.reader(avro_file)
            avro_data = list(avro_reader)

        # Convert the Avro data to a Pandas DataFrame
        df = pd.DataFrame(avro_data)
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
