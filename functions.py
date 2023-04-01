import pandas as pd

def dataframe_from_file(path, format):
    try:
        if format == "csv":
            return pd.read_csv(path)
        if format == "parquet":
            return pd.read_parquet(path)
    except FileNotFoundError:
        return None
    
def dataframe_to_file(df, path, format):
    if format == "csv":
        df.to_csv(f"{path}.csv", index=False)
    if format == "parquet":
        df.to_parquet(f"{path}.parquet", index=False)

# Convert redirect url to access token and state token
def url_to_auth_code(url):
    url_split = url.split("&")
    token = url_split[0][29:]
    state = url_split[1][6:]
    return token, state

def union_dataframes(df_old, df_new, drop_duplicates=True, drop_duplicates_columns=None):
    # If there is no old data (dataframe is None), just return the new data. Otherwise, union them and return.
    if df_old:
        if drop_duplicates:
            if drop_duplicates_columns:
                return pd.concat(df_old, df_new).drop_duplicates(subset=drop_duplicates_columns, keep="last")
            else:
                return pd.concat(df_old, df_new).drop_duplicates(keep="last")
        else:
            return pd.concat(df_old, df_new)
    else:
        return df_new
