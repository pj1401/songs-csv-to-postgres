"""
Extractor module for extracting data from files.
"""

import numpy as np
import pandas as pd
import h5py

def read_csv_data(file_path: str, chunk_size: int):
  return pd.read_csv(file_path, chunksize=chunk_size)

def read_hdf5_data(file_path: str, group_name: str, dataset_name="data"):
  f = h5py.File(file_path, "r")
  df = pd.DataFrame(np.array(f[group_name][dataset_name]))
  return df.rename(columns={"song_id": "track_id"})
