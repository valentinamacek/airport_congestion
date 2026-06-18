FROM flink:1.18.1-scala_2.12-java11

USER root

# Install Python 3.10 + build tools
RUN apt-get update && \
    apt-get install -y \
        python3.10 \
        python3.10-venv \
        python3.10-dev \
        python3-pip \
        curl \
        gcc \
        g++ \
        && \
    ln -sf /usr/bin/python3.10 /usr/bin/python3 && \
    ln -sf /usr/bin/python3.10 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel

# Install PyFlink and dependencies
RUN pip3 install --no-cache-dir \
    "apache-flink==1.18.1" \
    "psycopg2-binary==2.9.9" \
    "requests==2.31.0"

# Download Kafka connector JAR into Flink's lib folder
RUN curl -fSL \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar \
    -o /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar

WORKDIR /app
COPY flink_job.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

CMD ["/app/entrypoint.sh"]
