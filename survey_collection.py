# -*- coding: utf-8 -*-
import os
import requests
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text

# Fetch ALL configuration parameters dynamically from GitHub environment variables
MY_API_TOKEN = os.getenv("KOBO_TOKEN")
FORM_ASSET_ID = os.getenv("FORM_ASSET_ID")

SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_DB_NAME = os.getenv("SUPABASE_DB_NAME")
SUPABASE_PORT = os.getenv("SUPABASE_PORT")
SUPABASE_SCHEMA = os.getenv("SUPABASE_SCHEMA")
SUPABASE_TABLE_NAME = os.getenv("SUPABASE_TABLE_NAME")

servers = {
    "Global/Non-Humanitarian Server": "kf.kobotoolbox.org",
    "Humanitarian Server Cluster": "kobo.humanitarianresponse.info"
}

headers = {"Authorization": f"Token {MY_API_TOKEN}"}

def extract_lat_lon(location_str):
    if pd.isna(location_str):
        return None, None
    parts = str(location_str).split()
    if len(parts) >= 2:
        try:
            return float(parts[0]), float(parts[1])
        except ValueError:
            pass
    return None, None

def push_to_supabase(df_to_push, engine):
    if df_to_push.empty:
        print(f"No new data to push to target schema destination.")
        return

    try:
        if '_submission_time' in df_to_push.columns and df_to_push['_submission_time'].dt.tz is not None:
            df_to_push['_submission_time'] = df_to_push['_submission_time'].dt.tz_convert(None)

        with engine.connect() as connection:
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS \"{SUPABASE_SCHEMA}\""))
            connection.commit()
            print("Ensured schema exists.")

        df_to_push.to_sql(
            name=SUPABASE_TABLE_NAME,
            con=engine,
            schema=SUPABASE_SCHEMA,
            if_exists='append', 
            index=False 
        )
        print(f"✅ Successfully pushed {len(df_to_push)} new rows to Supabase!")
    except Exception as e:
        print(f"❌ Failed to push data to Supabase: {e}")

def run_data_pipeline(engine):
    print("\n--- Extracting Latest Kobo Submissions ---")
    current_df = None
    for server_name, domain in servers.items():
        api_url = f"https://{domain}/api/v2/assets/{FORM_ASSET_ID}/data/?format=json"
        print(f"Scanning cloud infrastructure endpoint...")
        try:
            response = requests.get(api_url, headers=headers)
            if response.status_code == 200:
                raw_json_data = response.json()
                submissions = raw_json_data.get('results', [])
                if len(submissions) > 0:
                    current_df = pd.DataFrame(submissions)
                    current_df.columns = [col.split('/')[-1] if '/' in col else col for col in current_df.columns]
                    print(f"✅ Connection successful! Fetched {len(current_df)} rows.")
                    break
        except Exception as e:
            print(f"Connection error encountered: {e}")

    if current_df is None or current_df.empty:
        print("No data retrieved from KoboToolbox.")
        return

    # --- Transformation Pipeline ---
    required_columns = ['Name', 'Age', 'Income_Source', 'Salary', 'Location', '_submission_time', 'Address', '_id']
    for col in required_columns:
        if col not in current_df.columns:
            current_df[col] = None

    cleaned_df = current_df[required_columns].copy()

    cleaned_df['Age'] = cleaned_df['Age'].astype(str).str.replace('_', '-')
    cleaned_df['Salary'] = pd.to_numeric(cleaned_df['Salary'], errors='coerce')
    cleaned_df['_submission_time'] = pd.to_datetime(cleaned_df['_submission_time'], errors='coerce')
    cleaned_df['_id'] = cleaned_df['_id'].astype(str)

    cleaned_df[['Latitude', 'Longitude']] = cleaned_df['Location'].apply(lambda x: pd.Series(extract_lat_lon(x)))

    # --- Incremental Deduplication ---
    existing_ids = set()
    try:
        with engine.connect() as connection:
            table_exists_query = text(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{SUPABASE_SCHEMA}' AND table_name = '{SUPABASE_TABLE_NAME}')")
            table_exists = connection.execute(table_exists_query).scalar()

            if table_exists:
                result = connection.execute(text(f"SELECT _id FROM \"{SUPABASE_SCHEMA}\".\"{SUPABASE_TABLE_NAME}\""))
                existing_ids = set([str(row[0]) for row in result.fetchall()])
                print(f"Found {len(existing_ids)} pre-existing keys in database.")
    except Exception as e:
        print(f"Database sync notice: {e}")

    new_rows_df = cleaned_df[~cleaned_df['_id'].isin(existing_ids)].copy()
    print(f"Target new rows detected: {len(new_rows_df)}")

    # --- Load Data ---
    push_to_supabase(new_rows_df, engine)
    print("--- Pipeline Flow Completed Successfully ---")

if __name__ == "__main__":
    DATABASE_URL = f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB_NAME}"
    try:
        db_engine = create_engine(DATABASE_URL)
        run_data_pipeline(db_engine)
    except Exception as e:
        print(f"Fatal Initialization Error: {e}")