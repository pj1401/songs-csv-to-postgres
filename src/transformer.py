"""
Transformer module for cleaning and joining data.
"""

import pandas as pd

def transform_csv(df: pd.DataFrame):
  print(df.info)

def transform_hdf5(df: pd.DataFrame):
  print(df.info)

def transform_chunk(df: pd.DataFrame, hdf5_df: pd.DataFrame):
  return pd.merge(
      df,
      hdf5_df,
      on="track_id",
      how="inner"
    )

def transform(csv_data, hdf5_df: pd.DataFrame):
  print(hdf5_df.info)
  for chunk in csv_data:
    print(transform_chunk(chunk, hdf5_df).info)
