# Use a base image with Python and Java installed (Flink requires Java)
# Eclipse Temurin provides a good base for Flink applications
FROM eclipse-temurin:11-jre-focal

# Set environment variables for Flink and Python
ENV FLINK_HOME=/opt/flink
ENV PATH=$PATH:$FLINK_HOME/bin
ENV PYTHON_HOME=/opt/venv
ENV PATH=$PYTHON_HOME/bin:$PATH
ENV PYFLINK_PYTHON_ARCHIVE=/opt/pyflink.zip
ENV PIP_NO_CACHE_DIR=off
ENV PYTHONUNBUFFERED=1

# Install necessary Python packages and Flink Python API
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-venv \
        openjdk-11-jdk \
        build-essential gcc && \
    rm -rf /var/lib/apt/lists/*

# Create a Python virtual environment
RUN python3 -m venv $PYTHON_HOME
RUN $PYTHON_HOME/bin/pip install --upgrade pip

# Install PyFlink and other dependencies
# Use a specific PyFlink version compatible with your Flink setup
# For local testing, Flink 1.17 is a common choice.
# This assumes you have Flink 1.17.x configured in your docker-compose if running as a service.
RUN $PYTHON_HOME/bin/pip install apache-flink==1.17.1

# Install other Python libraries required by your script
# CHANGE THIS LINE: Reference requirements.txt directly from the build context
COPY requirements.txt /tmp/requirements.txt
RUN $PYTHON_HOME/bin/pip install -r /tmp/requirements.txt

# Package the virtual environment for Flink's PyFlinkJobDescriptor
RUN cd $PYTHON_HOME && zip -r $PYFLINK_PYTHON_ARCHIVE .

# Copy your Flink application script
# CHANGE THIS LINE: Reference your script directly from the build context
COPY historical_aggregated.py /opt/flink_job/historical_aggregated.py

# Entrypoint to run the Flink job
# We're directly using the Flink Python executable which manages the Flink mini-cluster in standalone mode
# When you deploy to a Flink cluster, you'd change this to target the cluster.
CMD ["/opt/flink/bin/flink", "run", \
     "-py", "/opt/flink_job/historical_aggregated.py", \
     "-pyfs", "/opt/pyflink.zip"]