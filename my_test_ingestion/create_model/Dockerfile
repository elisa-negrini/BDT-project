# Use official Python image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# --- Spark specific environment variables ---
# Choose a Spark version and Hadoop version that are compatible with your needs.
# For pyspark 3.5.1, Spark 3.5.1 with Hadoop 3.3 is a good choice.
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_TGZ=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
ENV SPARK_HOME=/opt/spark 
ENV PATH="$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin"
ENV JAVA_HOME=/usr/lib/jvm/default-java
# --- End Spark specific environment variables ---

# Set working directory
WORKDIR /app

# Install system dependencies, INCLUDING JAVA
RUN apt-get update && apt-get install -y \
    build-essential \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    default-jre-headless \ 
    wget \                 
    tar \                  
    && rm -rf /var/lib/apt/lists/*

# --- Download and Install Apache Spark ---
# This layer handles the actual Spark binary installation
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}" -O /tmp/${SPARK_TGZ} && \
    tar -xzf /tmp/${SPARK_TGZ} -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm /tmp/${SPARK_TGZ}

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY . .

# Set the entry point
CMD ["python", "train.py"]