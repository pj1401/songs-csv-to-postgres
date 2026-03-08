import os
import pandas as pd
import h5py
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

from src.extractor import read_csv_data, read_hdf5_data, read_playcount_data
from src.transformer import transform, transform_playcount_data

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
CSV_LISTENING_HISTORY_PATH=os.getenv("CSV_LISTENING_HISTORY_PATH")
CHUNK_SIZE=int(os.getenv("CHUNK_SIZE"))

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
  hdf5_data = read_hdf5_data(HDF5_PATH)
  playcount_data = read_playcount_data(CSV_LISTENING_HISTORY_PATH, CHUNK_SIZE)

  # Transform
  total_playcount = transform_playcount_data(playcount_data)
  for chunk in csv_data:
    combined_data = transform(chunk, hdf5_data, total_playcount)

    # TODO: Seed database

  # Connect to database
  conn = connect_to_db()
  print("Connected to database")
  conn.close()
  print("Disconnected")

if __name__ == "__main__":
  main()
