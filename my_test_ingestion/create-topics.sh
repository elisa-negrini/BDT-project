#!/bin/bash

# Wait for Kafka to be ready
sleep 10

# Create topic with 3 partitions
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic bluesky

# Keep Kafka running
exec /etc/confluent/docker/run
