# Use the official Airflow image as the base
FROM apache/airflow:2.9.1

# Copy your requirements file into the container
COPY requirements.txt /

# Install the Python libraries
RUN pip install --no-cache-dir -r /requirements.txt