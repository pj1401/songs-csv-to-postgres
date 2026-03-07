"""
Extractor module for extracting data from files.
"""

import numpy as np
import pandas as pd
import h5py

def read_csv_data(file_path: str, chunk_size: int):
  return pd.read_csv(file_path, chunksize=chunk_size)

def read_hdf5_data(file_path: str):
  with h5py.File(file_path, "r") as f:
    # Read analysis group
    analysis_data = f["analysis"]["songs"][:]
    analysis_df = pd.DataFrame(analysis_data, columns=["track_id"])

    # Read metadata group
    metadata_data = f["metadata"]["songs"][:]
    metadata_df = pd.DataFrame(metadata_data, columns=["song_id", "release"])

    # Align by index
    hdf5_df = pd.concat([analysis_df, metadata_df], axis=1)

    return hdf5_df
