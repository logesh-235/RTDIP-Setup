
# onprem-rtdip-api/Dockerfile
FROM python:3.11-slim-bookworm

# Java 17 for Spark 3.5.x + procps (ps)
RUN apt-get update \
 && apt-get install -y openjdk-17-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:${PATH}"

# RTDIP SDK + FastAPI + matching PySpark + Delta
RUN pip install --no-cache-dir \
    fastapi uvicorn[standard] \
    rtdip-sdk==0.14.1 \
    pyspark==3.5.1 \
    delta-spark==3.0.0 \
    packaging \
    grpcio-status \
    grpcio>=1.48.1 \
    protobuf

WORKDIR /app
COPY app /app

EXPOSE 80
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
