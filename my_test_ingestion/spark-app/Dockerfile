FROM bitnami/spark:latest

USER root

# Install Python and pip (bitnami spark image might already have it)
RUN apt-get update && apt-get install -y python3 python3-pip

# Copy your app
WORKDIR /app
COPY app.py .

# Install pyspark
RUN pip3 install pyspark

CMD ["python3", "app.py"]

