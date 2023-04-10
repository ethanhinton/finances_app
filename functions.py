import pandas as pd
import os


def dataframe_from_file(path, format, storage_type):
    if storage_type == "oracle":
        config_path = os.getenv("ORACLE_CONFIG_PATH")
        try:
            if format == "csv":
                return pd.read_csv(path, storage_options={"config":config_path})
            if format == "parquet":
                return pd.read_parquet(path, storage_options={"config":config_path})
        except FileNotFoundError:
            return None
    else:
        try:
            if format == "csv":
                return pd.read_csv(path)
            if format == "parquet":
                return pd.read_parquet(path)
        except FileNotFoundError:
            return None
            
def dataframe_to_file(df, path, format, storage_type):
    if storage_type == "oracle":
        config_path = os.getenv("ORACLE_CONFIG_PATH")
        if format == "csv":
            df.to_csv(f"{path}.csv", index=False, storage_options={"config":config_path})
        if format == "parquet":
            df.to_parquet(f"{path}.parquet", index=False, storage_options={"config":config_path})
    else:
        if format == "csv":
            df.to_csv(f"{path}.csv", index=False)
        if format == "parquet":
            df.to_parquet(f"{path}.parquet", index=False)
               
# creates a path to save files to based on filename, the storage type, and the folder/bucket name
def create_path(filename, storage_type, folder):
    if storage_type == "oracle":
        return f"oci://{folder}/{filename}"
    elif storage_type == "local":
        return os.path.join(folder, filename)

# Convert redirect url to access token and state token
def url_to_auth_code(url):
    url_split = url.split("&")
    token = url_split[0][29:]
    state = url_split[1][6:]
    return token, state

def union_dataframes(df_old, df_new, drop_duplicates_columns=None):
    # If there is no old data (dataframe is None), just return the new data. Otherwise, union them and return.
    if df_old:
        if drop_duplicates_columns:
            return pd.concat(df_old, df_new).drop_duplicates(subset=drop_duplicates_columns, keep="last")
        else:
            return pd.concat(df_old, df_new)
    else:
        return df_new

