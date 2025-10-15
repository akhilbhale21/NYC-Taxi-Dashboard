# NYC Taxi Data

## Overview
The goal of this project was to develop an entire ETL Pipeline from start to finish. I chose the NYC Taxi dataset. 
I used technologies like streamlit to display our visuals, postgres to store our data, jupyter notebook to code it all, and docker. Then, I used the API connected to the dataset which was found in the nYC open data website. My API key looked like this: "https://data.cityofnewyork.us/resource/4b4i-vvec.json". 
<br>

## Postgres setup
- Created PostgreSQL database `taxi_data` using `psycopg2`.
- A normalized relational schema using lookup tables and foreign keys:
  - `vendors`, `rate_codes`, `payment_types`, `locations` (lookup tables)
  - `trips` (fact table referencing the lookups)
- Populated all lookup tables with meaningful values.
- Parsed and loaded cleaned taxi trip data into the normalized schema.
- Verified data loading with SQL joins and row count checks.

Created the ERD for our database to visually represent table relationships. This diagram helped guide our schema decisions. 


<br>


## How to Run the Project

1. Download the "proj docker" folder
2. Make a .env file which should look like this (change the actual values of the variables):
   ```python
    POSTGRES_USER=postgres
    POSTGRES_PASSWORD=admin
    POSTGRES_DB=taxi_data
    DATABASE_URL=postgresql://postgres:admin@db:5432/taxi_data
4. Next, run "docker-compose up --build"
5. Copy and paste the URL in a browser of your choice. Replace the 0.0.0.0 with localhost. So the URL should look like "http://localhost:8501". Below is a screenshot of the streamlit dashboard and what should come up on your screen too.

<br>
<br>

<img width="1494" alt="Screenshot 2025-05-06 at 4 45 37 PM" src="https://github.com/user-attachments/assets/dce77810-0f7a-4063-b029-324d49bd7fc1" />
<br>
