# Songs CSV to PostgreSQL

Load datasets into PostgreSQL.

## Files

Small subsets of the datasets are included in `seed/` to allow faster testing.

The actual files should be placed in the `data/` directory.
The CSV files can be downloaded here: [Million Song Dataset + Spotify + Last.fm](https://www.kaggle.com/datasets/undefinenull/million-song-dataset-spotify-lastfm)
The HDF5 file can be downloaded here: [http://millionsongdataset.com/pages/getting-dataset/, (Additional files, 7)](http://millionsongdataset.com/pages/getting-dataset/)

Update the path variables in the `.env` file:

```
CSV_PATH=data/Music Info.csv
CSV_LISTENING_HISTORY_PATH=data/User Listening History.csv
HDF5_PATH=data/msd_summary_file.h5
```
