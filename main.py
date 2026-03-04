import os
import pandas as pd
import h5py
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

POSTGRES_DB=os.getenv("POSTGRES_DB")
POSTGRES_USER=os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD=os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT=os.getenv("POSTGRES_PORT")

SQL_URI=os.getenv("SQL_URI")
SQL_TABLE=os.getenv("SQL_TABLE")

CSV_PATH=os.getenv("CSV_PATH")

HDF5_PATH=os.getenv("HDF5_PATH")

def connect_to_db():
    """Connect to PostgreSQL database."""
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=SQL_URI,
        port=POSTGRES_PORT
    )
    return conn

def main():
    # Connect to database
    conn = connect_to_db()
    print("Connected to database")
    conn.close()
    print("Disconnected")

if __name__ == "__main__":
    main()
