# #!/usr/bin/env python
# # coding: utf-8

# # In[1]:


# import pandas as pd
# from sqlalchemy import create_engine, text
# import psycopg2
# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# import os

# DB_HOST = os.getenv('DB_HOST', 'localhost')
# DB_PORT = os.getenv('DB_PORT', '5432')
# DB_NAME = os.getenv('DB_NAME', 'taxi_data')
# DB_USER = os.getenv('DB_USER', 'postgres')
# DB_PASSWORD = os.getenv('DB_PASSWORD', 'hello')


# print(f"Database: Host={DB_HOST}, Port={DB_PORT}, DB={DB_NAME}")


# # In[2]:


# def create_database():
#     try:
#         # Connect to the default PostgreSQL database (postgres)
#         conn = psycopg2.connect(
#             host=DB_HOST,
#             port=DB_PORT,
#             user=DB_USER,
#             password=DB_PASSWORD,
#             dbname='postgres'  # Connect to the default "postgres" database
#         )
#         conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
#         cursor = conn.cursor()

#         # Checking if the target database exists
#         cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}'")
#         exists = cursor.fetchone()

#         if not exists:
#             cursor.execute(f'CREATE DATABASE {DB_NAME}')
#             print(f"Database {DB_NAME} created successfully")
#         else:
#             print(f"Database {DB_NAME} already exists")

#         cursor.close()
#         conn.close()
#         return True
#     except Exception as e:
#         print(f"Error creating database: {e}")
#         return False

# # Create the database
# db_created = create_database()
# db_created


# # In[3]:

# # If DATABASE_URL is expected in the environment
# engine_url = os.getenv("DATABASE_URL")

# # Optional fallback for local testing
# if engine_url is None:
#     engine_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# engine = create_engine(engine_url)


# # engine_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
# # engine = create_engine(engine_url)

# def create_normalized_tables(engine):
#     try:
#         with engine.begin() as conn:
#             conn.execute(text("""
#                 DROP TABLE IF EXISTS trips CASCADE;
#                 DROP TABLE IF EXISTS vendors CASCADE;
#                 DROP TABLE IF EXISTS rate_codes CASCADE;
#                 DROP TABLE IF EXISTS payment_types CASCADE;
#                 DROP TABLE IF EXISTS locations CASCADE;
#             """))
#             conn.execute(text("""
#                 CREATE TABLE vendors (
#                     vendor_id INTEGER PRIMARY KEY,
#                     vendor_name VARCHAR(100)
#                 );
#                 CREATE TABLE rate_codes (
#                     rate_code_id INTEGER PRIMARY KEY,
#                     rate_code_name VARCHAR(100),
#                     description TEXT
#                 );
#                 CREATE TABLE payment_types (
#                     payment_type_id INTEGER PRIMARY KEY,
#                     payment_name VARCHAR(100),
#                     description TEXT
#                 );
#                 CREATE TABLE locations (
#                     "LocationID" INTEGER PRIMARY KEY,
#                     "Borough" VARCHAR(100),
#                     "Zone" VARCHAR(100),
#                     "service_zone" VARCHAR(100)
#                 );
#                 CREATE TABLE trips (
#                     trip_id SERIAL PRIMARY KEY,
#                     vendor_id INTEGER REFERENCES vendors(vendor_id),
#                     tpep_pickup_datetime TIMESTAMP,
#                     tpep_dropoff_datetime TIMESTAMP,
#                     passenger_count NUMERIC,
#                     trip_distance NUMERIC,
#                     rate_code_id INTEGER REFERENCES rate_codes(rate_code_id),
#                     store_and_fwd_flag VARCHAR(1),
#                     pickup_location_id INTEGER REFERENCES locations("LocationID"),
#                     dropoff_location_id INTEGER REFERENCES locations("LocationID"),
#                     payment_type_id INTEGER REFERENCES payment_types(payment_type_id),
#                     fare_amount NUMERIC(10,2),
#                     extra NUMERIC(10,2),
#                     mta_tax NUMERIC(10,2),
#                     tip_amount NUMERIC(10,2),
#                     tolls_amount NUMERIC(10,2),
#                     improvement_surcharge NUMERIC(10,2),
#                     total_amount NUMERIC(10,2),
#                     congestion_surcharge NUMERIC(10,2),
#                     airport_fee NUMERIC(10,2)
#                 );
#             """))
#         print("Tables created successfully")
#     except Exception as e:
#         print(f"Error creating tables: {e}")

# create_normalized_tables(engine)


# # In[4]:


# def populate_lookup_tables(engine):
#     vendors = pd.DataFrame({
#         'vendor_id': [1, 2, 6, 7],
#         'vendor_name': [
#             'Creative Mobile Technologies, LLC',
#             'Curb Mobility, LLC',
#             'Myle Technologies Inc',
#             'Helix'
#         ]
#     })
#     rate_codes = pd.DataFrame({
#         'rate_code_id': [1, 2, 3, 4, 5, 6, 99],
#         'rate_code_name': [
#             'Standard rate', 'JFK', 'Newark', 'Nassau or Westchester',
#             'Negotiated fare', 'Group ride', 'Null/unknown'
#         ],
#         'description': [
#             'Standard taxi rate', 'Flat fare to/from JFK', 'Flat fare to/from Newark',
#             'Trips to/from Nassau or Westchester', 'Negotiated fare',
#             'Group ride fare', 'Unknown rate code'
#         ]
#     })
#     payment_types = pd.DataFrame({
#         'payment_type_id': [0, 1, 2, 3, 4, 5, 6],
#         'payment_name': [
#             'Flex Fare trip', 'Credit Card', 'Cash', 'No charge',
#             'Dispute', 'Unknown', 'Voided trip'
#         ],
#         'description': [
#             'Flex Fare trip', 'Paid by credit card', 'Paid by cash',
#             'No charge', 'Fare in dispute', 'Unknown type', 'Voided fare'
#         ]
#     })

#     with engine.begin() as conn:
#         vendors.to_sql('vendors', conn, if_exists='append', index=False)
#         rate_codes.to_sql('rate_codes', conn, if_exists='append', index=False)
#         payment_types.to_sql('payment_types', conn, if_exists='append', index=False)
#     print("Lookup tables populated")

# populate_lookup_tables(engine)


# # In[5]:


# import os

# cleaned_csv_path = './data/cleaned_data.csv'

# # Check if the cleaned CSV file exists
# if os.path.exists(cleaned_csv_path):
#     print(f"File exists: {cleaned_csv_path}")
#     df = pd.read_csv(cleaned_csv_path)
# else:
#     print(f"File not found: {cleaned_csv_path}")


# # In[6]:


# df = pd.read_csv('/app/data/cleaned_data.csv')
# df['tpep_pickup_datetime'] = pd.to_datetime(df['pickup_date'] + ' ' + df['pickup_time'], errors='coerce')
# df['tpep_dropoff_datetime'] = pd.to_datetime(df['dropoff_date'] + ' ' + df['dropoff_time'], errors='coerce')

# def populate_locations_table(engine, df):
#     locs = pd.Series(list(df['pulocationid'].dropna().astype(int)) + list(df['dolocationid'].dropna().astype(int))).unique()
#     locs_df = pd.DataFrame({
#         'LocationID': locs,
#         'Borough': ['Unknown'] * len(locs),
#         'Zone': ['Unknown'] * len(locs),
#         'service_zone': ['Unknown'] * len(locs)
#     })
#     with engine.begin() as conn:
#         locs_df.to_sql('locations', conn, if_exists='append', index=False)
#     print(f"Inserted {len(locs_df)} locations")

# populate_locations_table(engine, df)


# # In[ ]:


# def transform_and_load_trips(engine, df):
#     df = df.rename(columns={
#         'vendorid': 'vendor_id',
#         'pulocationid': 'pickup_location_id',
#         'dolocationid': 'dropoff_location_id'
#     })
#     payment_map = {
#         'Flex Fare trip': 0,
#         'Credit Card': 1,
#         'Cash': 2,
#         'No charge': 3,
#         'Dispute': 4,
#         'Unknown': 5,
#         'Voided trip': 6
#     }
#     rate_map = {
#         'Standard rate': 1,
#         'JFK': 2,
#         'Newark': 3,
#         'Nassau or Westchester': 4,
#         'Negotiated fare': 5,
#         'Group ride': 6,
#         'Null/unknown': 99
#     }

#     df['payment_type_id'] = df['payment_type'].map(payment_map)
#     df['rate_code_id'] = df['rate_description'].map(rate_map)

#     required = ['vendor_id', 'rate_code_id', 'pickup_location_id', 'dropoff_location_id', 'payment_type_id']
#     df.dropna(subset=required, inplace=True)

#     for col in required:
#         df[col] = df[col].astype(int)

#     money_cols = [
#         'fare_amount', 'extra', 'mta_tax', 'tip_amount',
#         'tolls_amount', 'improvement_surcharge', 'total_amount',
#         'congestion_surcharge', 'airport_fee'
#     ]
#     for col in money_cols:
#         if col in df.columns:
#             df[col] = df[col].round(2)

#     with engine.begin() as conn:
#         df.to_sql('trips', conn, if_exists='replace', index=False)
#     print(f"Inserted {len(df)} trips")

# transform_and_load_trips(engine, df)


# # In[ ]:


# def verify_data_loading():
#     for tbl in ['vendors', 'rate_codes', 'payment_types', 'locations', 'trips']:
#         count = pd.read_sql(f'SELECT COUNT(*) FROM {tbl}', engine)
#         print(f"{tbl}: {count.iloc[0, 0]} records")

#     join_sample = pd.read_sql("""
#     SELECT t.vendor_id, v.vendor_name, t.fare_amount, t.total_amount
#     FROM trips t JOIN vendors v ON t.vendor_id = v.vendor_id
#     LIMIT 5
#     """, engine)
#     print("Sample join:")
#     print(join_sample)

# verify_data_loading()
# print("postgreSQL complete.")


# # In[ ]:





#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Use environment variables with fallbacks
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'taxi_data')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'hello')

# Optional: use full DATABASE_URL if provided
DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL:
    engine_url = DATABASE_URL
else:
    engine_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

print(f"Using database URL: {engine_url}")


def create_database():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname='postgres'  # Do not change this!
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}'")
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(f'CREATE DATABASE {DB_NAME}')
            print(f"Database {DB_NAME} created successfully.")
        else:
            print(f"Database {DB_NAME} already exists.")

        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error creating database: {e}")
        return False


# Step 1: Create the database
db_created = create_database()

# Step 2: Connect to the target DB and create engine
engine = create_engine(engine_url)


def create_normalized_tables(engine):
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                DROP TABLE IF EXISTS trips CASCADE;
                DROP TABLE IF EXISTS vendors CASCADE;
                DROP TABLE IF EXISTS rate_codes CASCADE;
                DROP TABLE IF EXISTS payment_types CASCADE;
                DROP TABLE IF EXISTS locations CASCADE;

                CREATE TABLE vendors (
                    vendor_id INTEGER PRIMARY KEY,
                    vendor_name VARCHAR(100)
                );
                CREATE TABLE rate_codes (
                    rate_code_id INTEGER PRIMARY KEY,
                    rate_code_name VARCHAR(100),
                    description TEXT
                );
                CREATE TABLE payment_types (
                    payment_type_id INTEGER PRIMARY KEY,
                    payment_name VARCHAR(100),
                    description TEXT
                );
                CREATE TABLE locations (
                    "LocationID" INTEGER PRIMARY KEY,
                    "Borough" VARCHAR(100),
                    "Zone" VARCHAR(100),
                    "service_zone" VARCHAR(100)
                );
                CREATE TABLE trips (
                    trip_id SERIAL PRIMARY KEY,
                    vendor_id INTEGER REFERENCES vendors(vendor_id),
                    tpep_pickup_datetime TIMESTAMP,
                    tpep_dropoff_datetime TIMESTAMP,
                    passenger_count NUMERIC,
                    trip_distance NUMERIC,
                    rate_code_id INTEGER REFERENCES rate_codes(rate_code_id),
                    store_and_fwd_flag VARCHAR(1),
                    pickup_location_id INTEGER REFERENCES locations("LocationID"),
                    dropoff_location_id INTEGER REFERENCES locations("LocationID"),
                    payment_type_id INTEGER REFERENCES payment_types(payment_type_id),
                    fare_amount NUMERIC(10,2),
                    extra NUMERIC(10,2),
                    mta_tax NUMERIC(10,2),
                    tip_amount NUMERIC(10,2),
                    tolls_amount NUMERIC(10,2),
                    improvement_surcharge NUMERIC(10,2),
                    total_amount NUMERIC(10,2),
                    congestion_surcharge NUMERIC(10,2),
                    airport_fee NUMERIC(10,2)
                );
            """))
        print("Tables created successfully.")
    except Exception as e:
        print(f"Error creating tables: {e}")


def populate_lookup_tables(engine):
    vendors = pd.DataFrame({
        'vendor_id': [1, 2, 6, 7],
        'vendor_name': [
            'Creative Mobile Technologies, LLC',
            'Curb Mobility, LLC',
            'Myle Technologies Inc',
            'Helix'
        ]
    })
    rate_codes = pd.DataFrame({
        'rate_code_id': [1, 2, 3, 4, 5, 6, 99],
        'rate_code_name': [
            'Standard rate', 'JFK', 'Newark', 'Nassau or Westchester',
            'Negotiated fare', 'Group ride', 'Null/unknown'
        ],
        'description': [
            'Standard taxi rate', 'Flat fare to/from JFK', 'Flat fare to/from Newark',
            'Trips to/from Nassau or Westchester', 'Negotiated fare',
            'Group ride fare', 'Unknown rate code'
        ]
    })
    payment_types = pd.DataFrame({
        'payment_type_id': [0, 1, 2, 3, 4, 5, 6],
        'payment_name': [
            'Flex Fare trip', 'Credit Card', 'Cash', 'No charge',
            'Dispute', 'Unknown', 'Voided trip'
        ],
        'description': [
            'Flex Fare trip', 'Paid by credit card', 'Paid by cash',
            'No charge', 'Fare in dispute', 'Unknown type', 'Voided fare'
        ]
    })

    with engine.begin() as conn:
        vendors.to_sql('vendors', conn, if_exists='append', index=False)
        rate_codes.to_sql('rate_codes', conn, if_exists='append', index=False)
        payment_types.to_sql('payment_types', conn, if_exists='append', index=False)
    print("Lookup tables populated.")


def load_cleaned_data():
    cleaned_csv_path = os.getenv('CLEANED_CSV_PATH', './data/cleaned_data.csv')
    if os.path.exists(cleaned_csv_path):
        print(f"Found cleaned CSV: {cleaned_csv_path}")
        return pd.read_csv(cleaned_csv_path)
    else:
        raise FileNotFoundError(f"Cleaned CSV not found at {cleaned_csv_path}")


def populate_locations_table(engine, df):
    locs = pd.Series(list(df['pulocationid'].dropna().astype(int)) + list(df['dolocationid'].dropna().astype(int))).unique()
    locs_df = pd.DataFrame({
        'LocationID': locs,
        'Borough': ['Unknown'] * len(locs),
        'Zone': ['Unknown'] * len(locs),
        'service_zone': ['Unknown'] * len(locs)
    })
    with engine.begin() as conn:
        locs_df.to_sql('locations', conn, if_exists='append', index=False)
    print(f"Inserted {len(locs_df)} locations.")


def transform_and_load_trips(engine, df):
    df = df.rename(columns={
        'vendorid': 'vendor_id',
        'pulocationid': 'pickup_location_id',
        'dolocationid': 'dropoff_location_id'
    })

    df['tpep_pickup_datetime'] = pd.to_datetime(df['pickup_date'] + ' ' + df['pickup_time'], errors='coerce')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['dropoff_date'] + ' ' + df['dropoff_time'], errors='coerce')

    payment_map = {
        'Flex Fare trip': 0, 'Credit Card': 1, 'Cash': 2,
        'No charge': 3, 'Dispute': 4, 'Unknown': 5, 'Voided trip': 6
    }
    rate_map = {
        'Standard rate': 1, 'JFK': 2, 'Newark': 3,
        'Nassau or Westchester': 4, 'Negotiated fare': 5,
        'Group ride': 6, 'Null/unknown': 99
    }

    df['payment_type_id'] = df['payment_type'].map(payment_map)
    df['rate_code_id'] = df['rate_description'].map(rate_map)

    required = ['vendor_id', 'rate_code_id', 'pickup_location_id', 'dropoff_location_id', 'payment_type_id']
    df.dropna(subset=required, inplace=True)
    for col in required:
        df[col] = df[col].astype(int)

    money_cols = [
        'fare_amount', 'extra', 'mta_tax', 'tip_amount',
        'tolls_amount', 'improvement_surcharge', 'total_amount',
        'congestion_surcharge', 'airport_fee'
    ]
    for col in money_cols:
        if col in df.columns:
            df[col] = df[col].round(2)

    with engine.begin() as conn:
        df.to_sql('trips', conn, if_exists='replace', index=False)
    print(f"Inserted {len(df)} trips.")


def verify_data_loading(engine):
    for tbl in ['vendors', 'rate_codes', 'payment_types', 'locations', 'trips']:
        count = pd.read_sql(f'SELECT COUNT(*) FROM {tbl}', engine)
        print(f"{tbl}: {count.iloc[0, 0]} records")

    join_sample = pd.read_sql("""
        SELECT t.vendor_id, v.vendor_name, t.fare_amount, t.total_amount
        FROM trips t
        JOIN vendors v ON t.vendor_id = v.vendor_id
        LIMIT 5
    """, engine)
    print("Sample join:")
    print(join_sample)


# Run all steps
create_normalized_tables(engine)
populate_lookup_tables(engine)

df = load_cleaned_data()
populate_locations_table(engine, df)
transform_and_load_trips(engine, df)
verify_data_loading(engine)

print("✅ PostgreSQL pipeline completed.")

