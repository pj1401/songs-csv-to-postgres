"""
Transformer module for cleaning and joining data.
"""

import h5py

def transform_hdf5(dset: h5py.Dataset):
  print(dset.shape)
