
# Use the official Python image
FROM python:3.11-slim

# Set environment variables to prevent Python from writing .pyc files and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system dependencies (for psycopg2-binary etc.)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

RUN mkdir -p /app/data

# Copy app source code
COPY . .

# Expose Streamlit's default port
EXPOSE 8501




# # Dockerfile.etl
# FROM python:3.10

# WORKDIR /app

# COPY requirements.txt .
# RUN pip install -r requirements.txt

# COPY load.py ./load.py
# COPY data ./data

# CMD ["python", "load.py"]

