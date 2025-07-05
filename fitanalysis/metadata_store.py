import duckdb
import pandas as pd
import os

class MetadataStore:
    def __init__(self, db_path='fit_metadata.db'):
        self.db_path = db_path
        self.conn = duckdb.connect(database=self.db_path, read_only=False)
        self._create_table()

    def _create_table(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS activities (
                activity_id VARCHAR PRIMARY KEY,
                activity_name VARCHAR,
                activity_type VARCHAR,
                start_time_gmt TIMESTAMP,
                start_time_local TIMESTAMP,
                distance DOUBLE,
                duration DOUBLE,
                calories INTEGER,
                average_hr INTEGER,
                max_hr INTEGER,
                average_power INTEGER,
                max_power INTEGER,
                file_path VARCHAR
            )
        """)

    def store_activity_metadata(self, activity_data: dict, file_path: str):
        activity_id = str(activity_data.get('activityId'))
        # Check if activity already exists
        result = self.conn.execute(f"SELECT activity_id FROM activities WHERE activity_id = '{activity_id}'").fetchone()
        if result:
            print(f"Activity {activity_id} already exists in metadata store. Skipping.")
            return

        self.conn.execute("""
            INSERT INTO activities VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            activity_id,
            activity_data.get('activityName'),
            activity_data.get('activityType'),
            activity_data.get('startTimeGMT'),
            activity_data.get('startTimeLocal'),
            activity_data.get('distance'),
            activity_data.get('duration'),
            activity_data.get('calories'),
            activity_data.get('averageHR'),
            activity_data.get('maxHR'),
            activity_data.get('averagePower'),
            activity_data.get('maxPower'),
            file_path
        ))
        print(f"Stored metadata for activity {activity_id}")

    def get_all_activities(self) -> pd.DataFrame:
        return self.conn.execute("SELECT * FROM activities").fetchdf()

    def close(self):
        self.conn.close()

