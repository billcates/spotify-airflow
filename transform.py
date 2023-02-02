import extract
import pandas as pd

df=extract.create_dataframe()

def Data_Quality(df):
    #check for data is present in dataframe or not
    if len(df)==0:
        raise ValueError("Empty dataframe. Please check")
        return False

    #check for null in any column
    if df.isnull().values.any():
        raise ValueError("Null Values are found.Please check")


def Transform_df(load_df):
    df=load_df.groupby(['timestamp','artist_name'],as_index=False).count()
    df.rename(columns={'played_at':'count'},inplace=True)
    df["id"]=df['timestamp'].astype(str) +"-"+ df["artist_name"]
    print("returning transformed df")
    return df[['id','timestamp','artist_name','count']]

