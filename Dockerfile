FROM apache/airflow:2.9.1

# 1. Install universal Java, wget, C-compilers, and Kafka C-libraries
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         default-jre \
         wget \
         build-essential \
         python3-dev \
         librdkafka-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# 2. Set universal Java path
ENV JAVA_HOME=/usr/lib/jvm/default-java

# 3. Create a Jars folder and download the pre-compiled SHADED dependencies
RUN mkdir -p /opt/airflow/jars
RUN wget -O /opt/airflow/jars/gcs-connector-hadoop3-shaded.jar https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.22/gcs-connector-hadoop3-2.2.22-shaded.jar
RUN wget -O /opt/airflow/jars/iceberg-spark-runtime.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar
RUN chown -R airflow:root /opt/airflow/jars

# 4. Install Python packages cleanly as root during build
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt