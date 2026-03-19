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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            FOREIGN KEY (track_id) REFERENCES tracks(track_id),
            FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
        );
        CREATE TABLE IF NOT EXISTS albums (
            album_id VARCHAR(50) PRIMARY KEY,
            album_name VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS tracks_albums (
            track_id VARCHAR(50),
            album_id VARCHAR(50),
            PRIMARY KEY (track_id, album_id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (track_id) REFERENCES tracks(track_id),
            FOREIGN KEY (album_id) REFERENCES albums(album_id)
        );
        CREATE TABLE IF NOT EXISTS artists_albums (
            artist_id VARCHAR(50),
            album_id VARCHAR(50),
            PRIMARY KEY (artist_id, album_id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
            FOREIGN KEY (album_id) REFERENCES albums(album_id)
        );
    """).format(table=sql.Identifier(table_name))

    cursor.execute(query)
    conn.commit()
    cursor.close()
    print(f"Created table: '{table_name}'")


def seed_database(conn, data: pd.DataFrame, table_name: str):
    """Seed the data into the PostgreSQL database."""
    seed_artists(conn, data[["artist_id", "artist_name"]])
    seed_albums(conn, data[["album_id", "album_name"]])
    seed_tracks(conn, data[["track_id", "name", "total_playcount"]])

    # Seed relationships
    seed_tracks_artists(conn, data[["track_id", "artist_id"]])
    seed_tracks_albums(conn, data[["track_id", "album_id"]])
    seed_artists_albums(conn, data[["artist_id", "album_id"]])


def seed_artists(conn, artists_data: pd.DataFrame):
    cursor = conn.cursor()
    for _, row in artists_data.drop_duplicates(subset=["artist_id"]).iterrows():
        query = """
            INSERT INTO artists (artist_id, artist_name)
            VALUES (%s, %s)
            ON CONFLICT (artist_id) DO NOTHING;
        """
        cursor.execute(query, (row["artist_id"], row["artist_name"]))
    conn.commit()
    cursor.close()
    print(f"Seeded {len(artists_data.drop_duplicates(subset=['artist_id']))} artists.")


def seed_albums(conn, albums_data: pd.DataFrame):
    cursor = conn.cursor()
    for _, row in albums_data.drop_duplicates(subset=["album_id"]).iterrows():
        query = """
            INSERT INTO albums (album_id, album_name)
            VALUES (%s, %s)
            ON CONFLICT (album_id) DO NOTHING;
        """
        cursor.execute(query, (row["album_id"], row["album_name"]))
    conn.commit()
    cursor.close()
    print(f"Seeded {len(albums_data.drop_duplicates(subset=['album_id']))} albums.")


def seed_tracks(conn, tracks_data: pd.DataFrame):
    cursor = conn.cursor()
    for _, row in tracks_data.iterrows():
        query = """
            INSERT INTO tracks (track_id, name, total_playcount)
            VALUES (%s, %s, %s)
            ON CONFLICT (track_id) DO NOTHING;
        """
        cursor.execute(
            query, (row["track_id"], row["name"], int(row["total_playcount"]))
        )
    conn.commit()
    cursor.close()
    print(f"Seeded {len(tracks_data)} tracks.")


def seed_tracks_artists(conn, tracks_artists_data: pd.DataFrame):
    cursor = conn.cursor()
    for _, row in tracks_artists_data.iterrows():
        query = """
            INSERT INTO tracks_artists (track_id, artist_id)
            VALUES (%s, %s)
            ON CONFLICT (track_id, artist_id) DO NOTHING;
        """
        cursor.execute(query, (row["track_id"], row["artist_id"]))
    conn.commit()
    cursor.close()
    print(f"Seeded {len(tracks_artists_data)} track-artist relationships.")


def seed_tracks_albums(conn, tracks_albums_data: pd.DataFrame):
    cursor = conn.cursor()
    for _, row in tracks_albums_data.iterrows():
        query = """
            INSERT INTO tracks_albums (track_id, album_id)
            VALUES (%s, %s)
            ON CONFLICT (track_id, album_id) DO NOTHING;
        """
        cursor.execute(query, (row["track_id"], row["album_id"]))
    conn.commit()
    cursor.close()
    print(f"Seeded {len(tracks_albums_data)} track-album relationships.")


def seed_artists_albums(conn, artists_albums_data: pd.DataFrame):
    cursor = conn.cursor()
    for _, row in artists_albums_data.iterrows():
        query = """
            INSERT INTO artists_albums (artist_id, album_id)
            VALUES (%s, %s)
            ON CONFLICT (artist_id, album_id) DO NOTHING;
        """
        cursor.execute(query, (row["artist_id"], row["album_id"]))
    conn.commit()
    cursor.close()
    print(f"Seeded {len(artists_albums_data)} artist-album relationships.")
