import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import datetime
import matplotlib.pyplot as plt
import seaborn as sns

st.set_page_config(
    page_title="NYC Taxi Trip Analysis Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

DB_HOST = 'localhost'
DB_PORT = '5432'  
DB_NAME = 'taxi_data'
DB_USER = 'postgres'
DB_PASSWORD = 'hello'  

@st.cache_resource
def get_connection():
    """Create a connection to the PostgreSQL database"""
    engine_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@host.docker.internal:{DB_PORT}/{DB_NAME}'

    #engine_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    return create_engine(engine_url)

@st.cache_data(ttl=600)  
def load_data(query):
    """Load data from PostgreSQL based on the provided query"""
    engine = get_connection()
    try:
        return pd.read_sql(text(query), engine)
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return pd.DataFrame()

st.title("NYC Taxi Trip Analysis Dashboard")
st.markdown("""

""")


row_count = load_data("SELECT COUNT(*) as count FROM trips")

# Check if the 'count' column exists before using it
if 'count' in row_count.columns:
    st.success(f"{int(row_count['count'].iloc[0])} taxi trips")
else:
    st.error("Column 'count' not found in the DataFrame.")

#st.success(f"{int(row_count['count'].iloc[0])} taxi trips")



st.sidebar.header("Filters")

date_query = """
SELECT 
    MIN(tpep_pickup_datetime) as min_date, 
    MAX(tpep_pickup_datetime) as max_date 
FROM trips
"""
date_range_df = load_data(date_query)

if not date_range_df.empty:
    min_date = pd.to_datetime(date_range_df['min_date'].iloc[0]).date()
    max_date = pd.to_datetime(date_range_df['max_date'].iloc[0]).date()
    
    st.sidebar.subheader("Date Range")
    selected_start_date = st.sidebar.date_input("Start date", min_date, min_value=min_date, max_value=max_date)
    selected_end_date = st.sidebar.date_input("End date", max_date, min_value=min_date, max_value=max_date)
    
    st.sidebar.subheader("Payment Type")
    payment_query = """
    SELECT pt.payment_type_id, pt.payment_name
    FROM payment_types pt
    JOIN trips t ON pt.payment_type_id = t.payment_type_id
    GROUP BY pt.payment_type_id, pt.payment_name
    """
    payment_types = load_data(payment_query)
    
    if not payment_types.empty:
        payment_options = payment_types['payment_name'].tolist()
        selected_payment_types = st.sidebar.multiselect(
            "Select payment types",
            options=payment_options,
            default=payment_options
        )
    
    st.sidebar.subheader("Trip Distance")
    distance_query = "SELECT MIN(trip_distance) as min_dist, MAX(trip_distance) as max_dist FROM trips"
    distance_range = load_data(distance_query)
    
    if not distance_range.empty:
        min_distance = float(distance_range['min_dist'].iloc[0])
        max_distance = float(distance_range['max_dist'].iloc[0])
        selected_distance = st.sidebar.slider(
            "Select trip distance range (miles)",
            min_value=min_distance,
            max_value=max_distance,
            value=(min_distance, max_distance)
        )
    
    where_clause = f"""
    WHERE CAST(tpep_pickup_datetime AS DATE) BETWEEN '{selected_start_date}' AND '{selected_end_date}'
    """
    
    if 'selected_payment_types' in locals() and selected_payment_types:
        payment_ids = payment_types[payment_types['payment_name'].isin(selected_payment_types)]['payment_type_id'].tolist()
        payment_ids_str = ','.join(map(str, payment_ids))
        where_clause += f" AND t.payment_type_id IN ({payment_ids_str})"
    
    if 'selected_distance' in locals():
        where_clause += f" AND trip_distance BETWEEN {selected_distance[0]} AND {selected_distance[1]}"

    tab1, tab2, tab3 = st.tabs([
        "Trip Overview", 
        "Payment Analysis",
        # "Time Patterns",
        # "Geography",
        "Surcharges & Fees"
    ])
    
    with tab1:
        st.header("Trip Overview")
        
        metrics_query = f"""
        SELECT 
            AVG(trip_distance) as avg_distance,
            AVG(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60) as avg_duration_min,
            AVG(fare_amount) as avg_fare,
            AVG(total_amount) as avg_total,
            AVG(passenger_count) as avg_passengers
        FROM trips t
        {where_clause}
        """
        
        metrics_df = load_data(metrics_query)
        
        if not metrics_df.empty:
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Avg Distance", f"{metrics_df['avg_distance'].iloc[0]:.2f} mi")
            
            with col2:
                st.metric("Avg Duration", f"{metrics_df['avg_duration_min'].iloc[0]:.1f} min")
            
            with col3:
                st.metric("Avg Fare", f"${metrics_df['avg_fare'].iloc[0]:.2f}")
            
            with col4:
                st.metric("Avg Total", f"${metrics_df['avg_total'].iloc[0]:.2f}")
                
            with col5:
                st.metric("Avg Passengers", f"{metrics_df['avg_passengers'].iloc[0]:.1f}")
        
        st.subheader("Trip Distance vs. Fare Amount")
        
        scatter_query = f"""
        SELECT 
            t.trip_distance, 
            t.fare_amount,
            pt.payment_name
        FROM trips t
        JOIN payment_types pt ON t.payment_type_id = pt.payment_type_id
        {where_clause}
        LIMIT 1000
        """
        
        scatter_df = load_data(scatter_query)
        
        if not scatter_df.empty:
            fig = px.scatter(
                scatter_df, 
                x="trip_distance", 
                y="fare_amount",
                color="payment_name",
                opacity=0.7,
                title="Relationship Between Trip Distance and Fare Amount",
                labels={
                    "trip_distance": "Trip Distance (miles)",
                    "fare_amount": "Fare Amount ($)",
                    "payment_name": "Payment Type"
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Trips by Vendor")
        
        vendor_query = f"""
        SELECT 
            v.vendor_name, 
            COUNT(*) as trip_count
        FROM trips t
        JOIN vendors v ON t.vendor_id = v.vendor_id
        {where_clause}
        GROUP BY v.vendor_name
        ORDER BY trip_count DESC
        """
        
        vendor_df = load_data(vendor_query)
        
        if not vendor_df.empty:
            fig = px.pie(
                vendor_df,
                values="trip_count",
                names="vendor_name",
                title="Distribution of Trips by Vendor",
                hole=0.4
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.header("Payment Analysis")
        
        st.subheader("Payment Type Distribution")
        
        payment_query = f"""
        SELECT 
            pt.payment_name,
            COUNT(*) as count
        FROM trips t
        JOIN payment_types pt ON t.payment_type_id = pt.payment_type_id
        {where_clause}
        GROUP BY pt.payment_name
        ORDER BY count DESC
        """
        
        payment_df = load_data(payment_query)
        
        if not payment_df.empty:
            fig = px.bar(
                payment_df,
                x="payment_name",
                y="count",
                color="payment_name",
                title="Number of Trips by Payment Type",
                labels={
                    "payment_name": "Payment Type",
                    "count": "Number of Trips"
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Average Tip by Payment Type")
        
        tip_query = f"""
        SELECT 
            pt.payment_name,
            AVG(t.tip_amount) as avg_tip,
            AVG(t.tip_amount / NULLIF(t.fare_amount, 0)) * 100 as tip_percentage
        FROM trips t
        JOIN payment_types pt ON t.payment_type_id = pt.payment_type_id
        {where_clause}
        GROUP BY pt.payment_name
        HAVING AVG(t.tip_amount) > 0
        ORDER BY avg_tip DESC
        """
        
        tip_df = load_data(tip_query)
        
        if not tip_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    tip_df,
                    x="payment_name",
                    y="avg_tip",
                    color="payment_name",
                    title="Average Tip Amount by Payment Type",
                    labels={
                        "payment_name": "Payment Type",
                        "avg_tip": "Average Tip ($)"
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.bar(
                    tip_df,
                    x="payment_name",
                    y="tip_percentage",
                    color="payment_name",
                    title="Average Tip Percentage by Payment Type",
                    labels={
                        "payment_name": "Payment Type",
                        "tip_percentage": "Tip Percentage (%)"
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.header("Surcharges & Fees")
        
        st.subheader("Average Surcharges and Fees")
        
        surcharges_query = f"""
        SELECT 
            AVG(improvement_surcharge) as avg_improvement_surcharge,
            AVG(congestion_surcharge) as avg_congestion_surcharge,
            AVG(airport_fee) as avg_airport_fee,
            AVG(tolls_amount) as avg_tolls
        FROM trips t
        {where_clause}
        """
        
        surcharges_df = load_data(surcharges_query)
        
        if not surcharges_df.empty:
            surcharges_melted = pd.melt(
                surcharges_df, 
                var_name="surcharge_type", 
                value_name="average_amount"
            )
            
            surcharges_melted['surcharge_type'] = surcharges_melted['surcharge_type'].map({
                'avg_improvement_surcharge': 'Improvement Surcharge',
                'avg_congestion_surcharge': 'Congestion Surcharge',
                'avg_airport_fee': 'Airport Fee',
                'avg_tolls': 'Tolls'
            })
            
            fig = px.bar(
                surcharges_melted,
                x="surcharge_type",
                y="average_amount",
                color="surcharge_type",
                title="Average Surcharges and Fees",
                labels={
                    "surcharge_type": "Type",
                    "average_amount": "Average Amount ($)"
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Breakdown of Total Fare")
        
        fare_breakdown_query = f"""
        SELECT 
            AVG(fare_amount) as base_fare,
            AVG(extra) as extra,
            AVG(mta_tax) as mta_tax,
            AVG(tip_amount) as tip,
            AVG(tolls_amount) as tolls,
            AVG(improvement_surcharge) as improvement_surcharge,
            AVG(congestion_surcharge) as congestion_surcharge,
            AVG(airport_fee) as airport_fee
        FROM trips t
        {where_clause}
        """
        
        fare_df = load_data(fare_breakdown_query)
        
        if not fare_df.empty:
            fare_melted = pd.melt(
                fare_df, 
                var_name="component", 
                value_name="amount"
            )
            
            fare_melted['component'] = fare_melted['component'].map({
                'base_fare': 'Base Fare',
                'extra': 'Extra',
                'mta_tax': 'MTA Tax',
                'tip': 'Tip',
                'tolls': 'Tolls',
                'improvement_surcharge': 'Improvement Surcharge',
                'congestion_surcharge': 'Congestion Surcharge',
                'airport_fee': 'Airport Fee'
            })
            
            fare_melted = fare_melted[fare_melted['amount'] > 0.01]
            
            fig = px.pie(
                fare_melted,
                values="amount",
                names="component",
                title="Average Fare Breakdown",
                hole=0.4
            )
            st.plotly_chart(fig, use_container_width=True)

st.sidebar.markdown("---")

if 'where_clause' not in locals():
    st.error(
        """
        could not load data 
        """
    )