
FROM bitnamilegacy/spark:3.4.1

USER root

COPY spark/requirements.txt /tmp/requirements.txt

# Cai PyTorch CPU
RUN /opt/bitnami/python/bin/python -m pip install \
    --no-cache-dir \
    torch==2.3.1 \
    torchvision==0.18.1 \
    --index-url https://download.pytorch.org/whl/cpu

# Cai cac thu vien du an
RUN /opt/bitnami/python/bin/python -m pip install \
    --no-cache-dir \
    -r /tmp/requirements.txt

# Loai bo OpenCV co giao dien, chi giu headless
RUN /opt/bitnami/python/bin/python -m pip uninstall -y \
    opencv-python \
    opencv-python-headless && \
    /opt/bitnami/python/bin/python -m pip install \
    --no-cache-dir \
    --no-deps \
    opencv-python-headless==4.10.0.84

# Kiem tra import
RUN /opt/bitnami/python/bin/python -c \
    "import clickhouse_connect, cv2, minio, pika, PIL, pyspark, torch, ultralytics; assert pyspark.__version__ == '3.4.1'; print('Spark worker dependencies OK')"

ENV PYSPARK_PYTHON=/opt/bitnami/python/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/bitnami/python/bin/python

ENV PYTHONPATH=/opt/bitnami/spark/python/lib/pyspark.zip:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip

USER 1001
