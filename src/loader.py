"""
Loader module for seeding the database.
"""

from psycopg2 import sql
import pandas as pd


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


# TODO: Create table if it does not exist.
