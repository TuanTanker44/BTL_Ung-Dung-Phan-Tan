FROM bitnamilegacy/spark:3.4.1

USER root

COPY spark/requirements.txt /tmp/requirements.txt

RUN /opt/bitnami/python/bin/python -m pip install \
    --no-cache-dir \
    -r /tmp/requirements.txt

ENV PYSPARK_PYTHON=/opt/bitnami/python/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/bitnami/python/bin/python

ENV PYTHONPATH=/opt/bitnami/spark/python/lib/pyspark.zip:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip

USER 1001