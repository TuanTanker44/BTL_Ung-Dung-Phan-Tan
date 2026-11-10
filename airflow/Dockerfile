# Java 17 runtime for Spark Submit
FROM eclipse-temurin:17-jre AS java-runtime

# Keep the existing Airflow version
FROM apache/airflow:2.6.3-python3.9

USER root

# Copy Java from a dedicated Java image.
# Avoid outdated Debian APT repositories.
COPY --from=java-runtime /opt/java/openjdk /opt/java/openjdk

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install uv without modifying Airflow's Python version
RUN pip install --no-cache-dir "uv==0.9.5"

# Create isolated Python 3.11 environment for Spark Driver
RUN uv python install 3.11 \
    && uv venv --python 3.11 /home/airflow/spark-venv \
    && uv pip install \
       --python /home/airflow/spark-venv/bin/python \
       "pyspark==3.4.1" \
       "pika>=1.3,<2" \
       "minio>=7.2,<8" \
       "clickhouse-connect>=0.7,<1" \
       "Pillow>=10,<12"

RUN /home/airflow/spark-venv/bin/python -c \
    "import clickhouse_connect, minio, pika, PIL, pyspark; assert pyspark.__version__ == '3.4.1'; print('Spark driver dependencies OK')"

ENV SPARK_HOME=/home/airflow/spark-venv/lib/python3.11/site-packages/pyspark
ENV PYSPARK_DRIVER_PYTHON=/home/airflow/spark-venv/bin/python
ENV PYSPARK_PYTHON=/opt/bitnami/python/bin/python
