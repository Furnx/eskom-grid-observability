# Use a slim, modern Python base
FROM python:3.10-slim

# Set environment variables for Python and Dagster
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Create directory structure
RUN mkdir -p $DAGSTER_HOME /app

# Set the working directory
WORKDIR /app

# Install system dependencies (needed for DuckDB/dbt in some environments)
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --default-timeout=100 --no-cache-dir -r requirements.txt

# Copy the entire project into the container
COPY . .

# Set up Dagster workspace config
RUN echo 'workspace:\n  - python_file: defs.py' > $DAGSTER_HOME/workspace.yaml

# Expose the Dagster webserver port
EXPOSE 3000

# Default command to run the webserver and the daemon (for the schedule)
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]