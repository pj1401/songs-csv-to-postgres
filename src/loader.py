"""
Loader module for seeding the database.
"""

from psycopg2 import sql
import pandas as pd

def create_table(conn, table_name: str):
    """Create the tracks table if it doesn't exist."""
    cursor = conn.cursor()
    query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            track_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(255),
            total_playcount BIGINT DEFAULT 0,  -- Use BIGINT (int64),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        );
        CREATE TABLE IF NOT EXISTS artists (
            artist_id VARCHAR(50) PRIMARY KEY,
            artist_name VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS tracks_artists (
            track_id VARCHAR(50),
            artist_id VARCHAR(50),
            PRIMARY KEY (track_id, artist_id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (track_id) REFERENCES tracks(track_id)),
            FOREIGN KEY (artist_id) REFERENCES artists(artist_id))
        );
        CREATE TABLE IF NOT EXISTS albums (
            album_id VARCHAR(50) PRIMARY KEY,
            album_name VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        );
        CREATE TABLE IF NOT EXISTS tracks_albums (
            track_id VARCHAR(50),
            album_id VARCHAR(50),
            PRIMARY KEY (track_id, album_id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (track_id) REFERENCES tracks(track_id)),
            FOREIGN KEY (album_id) REFERENCES albums(album_id))
        );
        CREATE TABLE IF NOT EXISTS artists_albums (
            artist_id VARCHAR(50),
            album_id VARCHAR(50),
            PRIMARY KEY (artist_id, album_id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (artist_id) REFERENCES artists(artist_id)),
            FOREIGN KEY (album_id) REFERENCES albums(album_id))
        );
    """).format(table=sql.Identifier(table_name))

    cursor.execute(query)
    conn.commit()
    cursor.close()
    print(f"Created table: '{table_name}'")

def seed_database(conn, data: pd.DataFrame, table_name: str):
    """Seed the merged data into the PostgreSQL database."""
    cursor = conn.cursor()

    for _, row in data.iterrows():
        query = sql.SQL("""
            INSERT INTO {table} (
                track_id, name, artist, album, total_playcount, artist_id, album_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (track_id) DO NOTHING;
        """).format(table=sql.Identifier(table_name))

        cursor.execute(
            query,
            (
                row["track_id"],
                row["name"],
                row["artist"],
                row["album_name"],
                row.get("total_playcount", 0),
                row.get("artist_id"),
                row.get("album_id"),
            ),
        )

    conn.commit()
    cursor.close()
    print(f"Seeded {len(data)} rows into {table_name}.")
