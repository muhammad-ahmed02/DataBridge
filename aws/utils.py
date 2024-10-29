from fastparquet import ParquetFile
import io
import os
import zipfile
import pandas as pd
import re
import fastavro

from data_bridge.settings import MEDIA_ROOT


def has_special_characters(string: str) -> bool:
    """
    conditional function used to check if a string parameter has special character in it or not.
        return True: string has only AlphaNumeric character with an underscore (_)
        return False: string has extra special character except underscore (_)
    """
    pattern = r"[^a-zA-Z0-9_\s]"
    if re.search(pattern, string):
        return True
    else:
        return False


def convert_bytes(byte_size) -> str:
    """
    Function used to convert number of bytes into respective measurement includes (KiloBytes, MegaBytes, GigaBytes)
    """
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


def make_tuple(string: str, size=None) -> tuple:
    return string, string + f"{' | Size: '+convert_bytes(size) if size else ''}"


def check_unique_tuple(new_tuple, tuple_list) -> bool:
    """
    check if the new tuple given as parameter already present in the tuple list given.
    """
    new_tuple_first_element = new_tuple[0]
    existing_first_elements = {t[0] for t in tuple_list}

    if new_tuple_first_element not in existing_first_elements:
        return True
    return False


def format_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Formats the pandas Dataframe e.g replacing ' ' with '_' and filling NaN or null values.
    """
    df.columns = df.columns.str.replace(" ", "_")
    if "Unnamed:_0" in df.columns:
        df.drop("Unnamed:_0", axis="columns", inplace=True)
    df.fillna("", inplace=True)
    return df


def get_file_df(con, bucket, table) -> tuple:
    """
    Reading files from S3 and returning dataframe of the data.
    Constraint:
        File should have size less than 1MB.
    """
    response = con.get_object(Bucket=bucket, Key=table)
    size = response['ContentLength']
    if size > (1024*1024):
        return False, "File too large to get schema! Try with files less than 1MB"
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
        con.download_file(bucket, table, table)
        # Open the Avro file and read it as a Pandas DataFrame
        with open(table, 'rb') as avro_file:
            avro_reader = fastavro.reader(avro_file)
            avro_data = list(avro_reader)
        # Convert the Avro data to a Pandas DataFrame
        df = pd.DataFrame(avro_data)
        os.remove(table)
    else:
        df = pd.DataFrame()
    df = format_df(df)
    return True, df


def checkdtype(val) -> str:
    """
    returning datatype of given value parameter.
    """
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


def find_files_in_s3(con, bucket, ext) -> list:
    """
    Function to get list of files in a bucket given from S3.
    """
    responses = con.list_objects_v2(Bucket=bucket)

    files = list()
    for response in responses.get("Contents", []):
        var = response["Key"]
        var_parts = var.split("/")
        if len(var_parts) > 1:
            continue
        else:
            if var.endswith(ext):
                files.append(make_tuple(var, size=response['Size']))
    return files


def create_zip_folder(files_path: list, files_name: list, folder_name=None) -> str:
    """
    creating zip folder of given files in media folder.
    """
    default_zip_folder_name = 'file.zip'
    zip_folder_name = folder_name+".zip" if folder_name else default_zip_folder_name
    zip_file = zipfile.ZipFile(os.path.join(MEDIA_ROOT, zip_folder_name), 'w')

    for path, name in zip(files_path, files_name):
        zip_file.write(path, name)

    zip_file.close()
    return zip_folder_name


def get_folder_schema_in_s3(con, bucket, table, ext) -> tuple:
    """
    Function to read schema of desired folder.
        Schema will be read for the specific file in the folder matching the extension given.
    """
    files = con.list_objects_v2(Bucket=bucket, Prefix=table)
    for file in sorted(files.get("Contents", []), key=lambda x: x['LastModified']):
        if file['Key'].endswith(ext):
            cond, df = get_file_df(con, bucket, table=file['Key'])
            if cond:
                schema = get_schema(df, file['Key'])
                return cond, schema
    return False, "File too large to get schema! Try with files less than 1MB"


def get_folder_df(con, bucket, table, ext) -> tuple:
    """
    Function to read schema of desired folder.
        Schema will be read for the specific file in the folder matching the extension given.
    """
    files = con.list_objects_v2(Bucket=bucket, Prefix=table)
    df = pd.DataFrame()
    cond = False
    for file in sorted(files.get("Contents", []), key=lambda x: x['LastModified']):
        if file["Key"].endswith(ext):
            cond, _df = get_file_df(con, bucket, table=file['Key'])
            if cond:
                df = pd.concat([df, _df])

    return cond, df


def get_schema(df: pd.DataFrame, table: str) -> dict:
    """
    returning schema of given DataFrame.
    """
    schema = dict()
    columns_list = list()

    for col in df.columns:
        column = dict()
        column["column"] = col
        column["datatype"] = checkdtype(df[col].dtype)
        columns_list.append(column)

    schema[table.split("/")[0]] = columns_list
    return schema


def get_list(d):

    if isinstance(d, list):
        return d

    for key, value in d.items():
        if isinstance(value, dict):
            return get_list(value)
        else:
            return value


def convert_schema_sql(schema: dict):
    """
     Convert dict schema into string format for sql query.
    """
    schema = get_list(schema)
    schema_sql = str()
    for idx, scheme in enumerate(schema):
        schema_sql += f"{scheme.get('column')} {scheme.get('datatype').upper()}"
        if idx < len(schema)-1:
            schema_sql += ", "
    return schema_sql
