#!/usr/bin/env python
# coding: utf-8

# In[1]:


## Display in Notebook
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"
from IPython.display import display

import pandas as pd
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_colwidth', None)  # Show full column width

import numpy as np
import os
from collections import Counter
from sqlalchemy import create_engine
import re
from sklearn.feature_extraction.text import CountVectorizer


# # first 5 cols

# In[2]:


df = pd.read_csv('data/extracted_data.csv')
print("Data loaded successfully!")
df.info()  # Display DataFrame information


# In[3]:


#drop duplicates- if any exist
df = df.drop_duplicates()
print(f'Number unique rows: {len(df)}')


# ### missing values

# In[4]:


# Step 2: Identify and Standardize Missing Values

# Replace all NaN values with Pandas' NA representation
df = df.replace({np.nan: pd.NA})

# Count the number of missing values in each column
missing_values = df.isna().sum()

# Count the number of rows with at least one missing value
num_rows_with_missing = df.isnull().any(axis=1).sum()

# Display results
print(f" Number of Rows with Missing Values: {num_rows_with_missing}\n")

print(" Missing Values Count Per Column (Before Cleaning):")
print(missing_values)


# ### pickup_datetime 

# In[5]:


print(df[['tpep_pickup_datetime']].head(20))


# In[6]:


import pandas as pd

# Your function
def standardize_birthdate(date):
    try:
        if pd.isna(date) or date in ["None", "nan", ""]:  # Handle missing values
            return "01/01/2000"
        
        date = str(date).strip()
        if "." in date:  # If the format is YYYY.MM.DD
            return pd.to_datetime(date, format="%Y.%m.%d").strftime("%m/%d/%Y")
        else:  # Assume the format is already a valid date
            return pd.to_datetime(date).strftime("%m/%d/%Y")
    except:
        return "01/01/2000"  # Replace invalid dates with default

# Convert datetime column to datetime dtype if it's not already
df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')

# Split into date and time
df['pickup_date'] = df['tpep_pickup_datetime'].dt.date.astype(str)
df['pickup_time'] = df['tpep_pickup_datetime'].dt.time.astype(str)

# Standardize the date format
df['pickup_date'] = df['pickup_date'].apply(standardize_birthdate)

# Final output
print(df[['tpep_pickup_datetime', 'pickup_date', 'pickup_time']].head(10))


# ### dropoff_datetime

# In[7]:


# Convert datetime column to datetime dtype if it's not already
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce')

# Split into date and time
df['dropoff_date'] = df['tpep_dropoff_datetime'].dt.date.astype(str)
df['dropoff_time'] = df['tpep_dropoff_datetime'].dt.time.astype(str)

# Standardize the date format
df['dropoff_date'] = df['dropoff_date'].apply(standardize_birthdate)

# Final output
print(df[['tpep_dropoff_datetime', 'dropoff_date', 'dropoff_time']].head(10))


# ### vendor id

# In[8]:


print(df[['vendorid']].head(10))
# Check for vendorid values not equal to 1 or 2
invalid_vendor_ids = df[~df['vendorid'].isin([1, 2])]

# Display them (if any)
print(invalid_vendor_ids[['vendorid']])

#vendorid is already clean


# ### passenger count

# In[9]:


print(df[['passenger_count']].head(10))
# Find rows where passenger_count is not between 1 and 6
invalid_passenger_counts = df[~df['passenger_count'].between(1, 6)]
print("Number of invalid passenger counts:", len(invalid_passenger_counts))


# In[10]:


# Remove rows where passenger_count is 0
df = df[df['passenger_count'] != 0]

# Optional: Reset the index if you want a clean index after removal
df.reset_index(drop=True, inplace=True)


# In[11]:


#count invalid passenger_counts again to check if dataset is clean
invalid_passenger_counts = df[~df['passenger_count'].between(1, 6)]
print("Number of invalid passenger counts:", len(invalid_passenger_counts))


# ### trip distance

# In[12]:


print(df[['trip_distance']].head(10))


# In[13]:


# Count rows where trip_distance is 0
zero_distance_count = (df['trip_distance'] == 0).sum()
print("Number of rows with trip_distance = 0:", zero_distance_count)


# In[14]:


# Remove rows where trip_distance is 0
df = df[df['trip_distance'] != 0]

# Optional: Reset the index if you want a clean index after removal
df.reset_index(drop=True, inplace=True)

zero_distance_count = (df['trip_distance'] == 0).sum()
print("Number of rows with trip_distance = 0:", zero_distance_count)


# ### remove rows where dropoff < pickup

# In[15]:


# Ensure the datetime columns are in proper datetime format
df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce')

# Find rows where dropoff is not after pickup
invalid_datetime_rows = df[df['tpep_dropoff_datetime'] <= df['tpep_pickup_datetime']]

# Display the rows
print(invalid_datetime_rows[['tpep_pickup_datetime', 'tpep_dropoff_datetime']])

# Optional: Count how many
print("Number of rows where dropoff is not after pickup:", len(invalid_datetime_rows))


# In[16]:


# Remove rows where dropoff is not after pickup
df = df[df['tpep_dropoff_datetime'] > df['tpep_pickup_datetime']]

# Optional: Reset index
df.reset_index(drop=True, inplace=True)


# # next 4 cols 

# In[17]:


# Preview the columns you're cleaning
df[["ratecodeid", "store_and_fwd_flag", "pulocationid", "dolocationid"]].head()


# In[18]:


# Define the mapping
ratecode_map = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau/Westchester",
    5: "Negotiated fare",
    6: "Group ride",
    99: "Unknown"
}

# Convert to numeric first (if not already)
df['ratecodeid'] = pd.to_numeric(df['ratecodeid'], errors='coerce')

# Map the values to descriptions
df['rate_description'] = df['ratecodeid'].map(ratecode_map)

# Drop the original column if no longer needed
df.drop(columns=['ratecodeid', 'tpep_dropoff_datetime', 'tpep_pickup_datetime'], inplace=True)


# In[ ]:





# # next 5 cols

# In[19]:


## cleans payment_type by creaing a map of the existing numeric values to the actual payment type
def clean_payment_type(df):
    payment_type_map = {
        0: "Flex Fare trip",
        1: "Credit Card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip"
    }
    df['payment_type'] = pd.to_numeric(df['payment_type'], errors='coerce')
    df['payment_type'] = df['payment_type'].map(payment_type_map)
    
    return df

## clean fare_amount, extra, mta_tax, tip_amount
def clean_fare_and_related_columns(df):
    numeric_cols = ['fare_amount', 'extra', 'mta_tax', 'tip_amount']
    
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].round(2)
        df = df[df[col] >= 0] 
    
    return df    


# In[20]:


df = clean_payment_type(df)
df = clean_fare_and_related_columns(df)


# In[21]:


df.drop(columns=['store_and_fwd_flag'], inplace=True)


# # last 5 cols

# In[22]:


# cleaning columns: tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee

columns_to_clean = [
    'tolls_amount', 
    'improvement_surcharge', 
    'total_amount', 
    'congestion_surcharge', 
    'airport_fee'
]

# converting all cols to numeric and handle missing vals
for col in columns_to_clean:
    df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # NaN values
    null_count = df[col].isna().sum()
    if null_count > 0:
        print(f"{null_count} missing values in {col}")
        # filled with 0
        df[col] = df[col].fillna(0)
    
    # making sure all values are non negative
    neg_count = (df[col] < 0).sum()
    if neg_count > 0:
        print(f" {neg_count} negatives values in {col}")
        df[col] = df[col].clip(lower=0)
    
    # round to 2 decimal places
    df[col] = df[col].round(2)

print()
print("cleaned cols:")
display(df[columns_to_clean].describe())


# In[23]:


# checking vals for improvement_surcharge
# from NYC TLC data dictionary and documentation:
# $0.30 surcharge from 2015 - 2018
# $0.50 prior to February 2022
# $1.00 after February 2022
# $0.00 for some exempt trips
print("improvement_surcharge value counts:")
display(df['improvement_surcharge'].value_counts().sort_index())

# checking for unusual values outside the expected ranges
expected_improvement_values = [0.0, 0.3, 0.5, 1.0]
mask = ~df['improvement_surcharge'].isin(expected_improvement_values)
if mask.sum() > 0:
    print(f"{mask.sum()} records with unexpected values:")
    display(df.loc[mask, 'improvement_surcharge'].value_counts())
    
    # replacing to the nearest expected value
    most_common_value = df.loc[~mask, 'improvement_surcharge'].mode()[0]
    df.loc[mask, 'improvement_surcharge'] = most_common_value
    print(f"fixed unexpected values by setting them to {most_common_value}")


# In[24]:


# congestion_surcharge
# from NYC TLC regulations:
# $2.50 for standard rides in congestion zone
# $2.75 for shared rides
# $0.00 for exempt trips or trips outside the congestion zone
print("congestion_surcharge value counts:")
display(df['congestion_surcharge'].value_counts().sort_index())

# checking for unexpected congestion surcharge vals
expected_congestion_values = [0.0, 2.5, 2.75]
mask = ~df['congestion_surcharge'].isin(expected_congestion_values)
if mask.sum() > 0:
    print(f"{mask.sum()} records with unexpected vlaues:")
    display(df.loc[mask, 'congestion_surcharge'].value_counts())
    
    most_common_value = df.loc[~mask, 'congestion_surcharge'].mode()[0]
    df.loc[mask, 'congestion_surcharge'] = most_common_value
    print(f"fixed unexpected values by setting them to {most_common_value}")


# In[25]:


# values for airport_fee
# from NYC TLC regulations:
# $1.25 for pickups at JFK or LaGuardia airports
# $0.00 for all other trips
print("airport_fee value counts:")
display(df['airport_fee'].value_counts().sort_index())

# checking for unexpected airport fee vals
expected_airport_values = [0.0, 1.25]
mask = ~df['airport_fee'].isin(expected_airport_values)
if mask.sum() > 0:
    print(f"{mask.sum()} records with unexpected airport_fee values:")
    display(df.loc[mask, 'airport_fee'].value_counts())
    
    most_common_value = df.loc[~mask, 'airport_fee'].mode()[0]
    df.loc[mask, 'airport_fee'] = most_common_value
    print(f"Fixed unexpected values by setting them to {most_common_value}")


# In[26]:


# correcting total_amount by checking against the sum of components

component_columns = [
    'fare_amount', 'extra', 'mta_tax', 'tip_amount', 
    'tolls_amount', 'improvement_surcharge', 'congestion_surcharge', 'airport_fee'
]

for col in component_columns:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(2)

# calculating the expected total
df['calculated_total'] = df[component_columns].sum(axis=1).round(2)

# comparing with the actual total_amount
df['difference'] = (df['total_amount'] - df['calculated_total']).abs().round(2)

# records with significant differences
discrepancies = df[df['difference'] > 0.01]

print(f"{len(discrepancies)} records where total_amount doesn't match the sum of components")
if len(discrepancies) > 0:
    print("sample:")
    display(discrepancies[['total_amount', 'calculated_total', 'difference']].head())
    
    df.loc[df['difference'] > 0.01, 'total_amount'] = df.loc[df['difference'] > 0.01, 'calculated_total']
    print("total amount fixed using calculated total amount")

df = df.drop(['calculated_total', 'difference'], axis=1)


# In[27]:


print("cleaned data sample:")
display(df.head())

monetary_columns = [
    'fare_amount', 'extra', 'mta_tax', 'tip_amount', 
    'tolls_amount', 'improvement_surcharge', 'total_amount', 
    'congestion_surcharge', 'airport_fee'
]

# rounding to have 2 decimal places
for col in monetary_columns:
    if col in df.columns:
        df[col] = df[col].round(2)

print("\ndecimal precision in columns:")
display(df[monetary_columns].head())


# In[28]:


df.to_csv('/app/data/cleaned_data.csv', index=False)


# In[ ]:




