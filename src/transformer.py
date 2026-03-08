"""
Transformer module for cleaning and joining data.
"""

import pandas as pd

def transform_csv(df: pd.DataFrame):
  print(df.info)

def transform_hdf5(df: pd.DataFrame):
  print(df.info)

def transform_chunk(df: pd.DataFrame, hdf5_df: pd.DataFrame):
  # Normalise track_id
  df["track_id"] = df["track_id"].astype("str").str.strip().str.upper()
  hdf5_df["track_id"] = hdf5_df["track_id"].astype("str").str.strip().str.upper()

  # Merge
  merged = df.merge(hdf5_df, on="track_id", how="inner")

  # Normalise new columns
  merged["song_id"] = merged["song_id"].astype("str").str.strip()
  merged["release"] = merged["release"].astype("str").str.strip()
  merged["release_7digitalid"] = merged["release_7digitalid"].astype("str").str.strip()
  merged["artist_id"] = merged["artist_id"].astype("str").str.strip()

  # Rename columns
  merged = merged.rename(columns={"release": "album_name"})
  merged = merged.rename(columns={"release_7digitalid": "album_id"})

  print(f"Merged shape: {merged.shape[0]}")
  print("Merged sample rows:\n", merged.head())

  return merged

def transform(csv_data, hdf5_df: pd.DataFrame):
  for chunk in csv_data:
    transform_chunk(chunk, hdf5_df)
