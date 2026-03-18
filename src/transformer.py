"""
Transformer module for cleaning and joining data.
"""

from collections.abc import Iterator

import pandas as pd


def transform_playcount_data(playcount_data: Iterator[pd.DataFrame]) -> pd.DataFrame:
    total_playcount = pd.DataFrame()
    for chunk in playcount_data:
        chunk["track_id"] = chunk["track_id"].astype("str").str.strip().str.upper()
        chunk = chunk.groupby("track_id")["playcount"].sum().reset_index()
        total_playcount = (
            pd.concat([total_playcount, chunk])
            .groupby("track_id")["playcount"]
            .sum()
            .reset_index()
        )
    return total_playcount


def merge(
    df: pd.DataFrame, hdf5_df: pd.DataFrame, total_playcount: pd.DataFrame
) -> pd.DataFrame:
    # Normalise track_id
    df["track_id"] = df["track_id"].astype("str").str.strip().str.upper()
    hdf5_df["track_id"] = hdf5_df["track_id"].astype("str").str.strip().str.upper()
    total_playcount["track_id"] = (
        total_playcount["track_id"].astype("str").str.strip().str.upper()
    )

    # Merge
    merged = df.merge(hdf5_df, on="track_id", how="inner")
    merged = merged.merge(total_playcount, on="track_id", how="left")

    return merged


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Strip whitespaces and set column names to lowercase.
    df.columns = [col.strip().lower() for col in df.columns]

    # Normalise values from the hdf5 data.
    df["song_id"] = df["song_id"].astype("str").str.strip()
    df["release"] = df["release"].astype("str").str.strip()
    df["release_7digitalid"] = df["release_7digitalid"].astype("str").str.strip()
    df["artist_id"] = df["artist_id"].astype("str").str.strip()
    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.rename(columns={"artist": "artist_name"})
    df = df.rename(columns={"release": "album_name"})
    df = df.rename(columns={"release_7digitalid": "album_id"})
    df = df.rename(columns={"playcount": "total_playcount"})
    return df


def replace_NaN(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["total_playcount"] = df["total_playcount"].fillna(0)
    df["total_playcount"] = df["total_playcount"].astype("int64")
    return df


def transform(
    csv_df: Iterator[pd.DataFrame],
    hdf5_df: pd.DataFrame,
    total_playcount: Iterator[pd.DataFrame],
):
    merged = merge(csv_df, hdf5_df, total_playcount)
    normalized = normalize(merged)
    renamed = rename_columns(normalized)
    return replace_NaN(renamed)
