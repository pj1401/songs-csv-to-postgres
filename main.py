import os
import pandas as pd
import h5py
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

from src.extractor import read_csv_data, read_hdf5_data
from src.transformer import transform

# Load environment variables
load_dotenv()

POSTGRES_DB=os.getenv("POSTGRES_DB")
POSTGRES_USER=os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD=os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT=os.getenv("POSTGRES_PORT")
POSTGRES_HOST=os.getenv("POSTGRES_HOST")

SQL_TABLE=os.getenv("SQL_TABLE")

CSV_PATH=os.getenv("CSV_PATH")
HDF5_PATH=os.getenv("HDF5_PATH")
CHUNK_SIZE=int(os.getenv("CHUNK_SIZE"))
HDF5_GROUP_NAME=os.getenv("HDF5_GROUP_NAME")
HDF5_DATASET_NAME=os.getenv("HDF5_DATASET_NAME")

def connect_to_db():
  """Connect to PostgreSQL database."""
  conn = psycopg2.connect(
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    host=POSTGRES_HOST,
    port=POSTGRES_PORT
  )
  return conn

def main():
  # Read data
  csv_data = read_csv_data(CSV_PATH, CHUNK_SIZE)
  hdf5_data = read_hdf5_data(HDF5_PATH, HDF5_GROUP_NAME, HDF5_DATASET_NAME)
  combined_data = transform(csv_data, hdf5_data)

  # Connect to database
  conn = connect_to_db()
  print("Connected to database")
  conn.close()
  print("Disconnected")

if __name__ == "__main__":
  main()
