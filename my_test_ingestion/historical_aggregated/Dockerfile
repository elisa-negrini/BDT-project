# Use official Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

COPY historical_aggregated.py /app/historical_aggregated.py
COPY requirements.txt /app/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the script
CMD ["python", "historical_aggregated.py"]
