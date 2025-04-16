# Use Ubuntu base image
FROM ubuntu:22.04

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install core tools & Python
RUN apt-get update && \
    apt-get install -yqq --no-install-recommends \
        python3 python3-pip \
        openjdk-11-jdk \
        wget curl net-tools nano sqlite3 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# JAVA ENV
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin


# Install Spark
ENV SPARK_VERSION=3.5.1
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Install Kafka
ENV KAFKA_VERSION=3.6.0
ENV SCALA_VERSION=2.13
RUN wget https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar -xzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    mv kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka && \
    rm kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz
ENV KAFKA_HOME=/opt/kafka
ENV PATH=$PATH:$KAFKA_HOME/bin


# Copy Python requirements
COPY requirements.txt /app/
RUN pip3 install --upgrade pip && pip3 install -r /app/requirements.txt

# Set working directory
WORKDIR /app

# Default command
CMD ["/bin/bash"]
