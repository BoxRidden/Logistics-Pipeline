FROM apache/airflow:2.9.1

# 1. Install universal Java
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends default-jre \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# 2. Set universal Java path
ENV JAVA_HOME=/usr/lib/jvm/default-java

# 3. Install Python packages
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt