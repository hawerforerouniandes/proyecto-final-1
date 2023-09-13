#!/bin/bash

# URL to download Kafka
KAFKA_URL="https://dlcdn.apache.org/kafka/3.4.1/kafka_2.13-3.4.1.tgz"

# Destination directory and temporary file
DEST_DIR="/opt"
TEMP_FILE="$DEST_DIR/kafka.tgz"

# Create destination directory if it doesn't exist
mkdir -p $DEST_DIR

# Download Kafka archive
echo "Downloading Kafka..."
curl -o $TEMP_FILE $KAFKA_URL

# Unzip Kafka archive
echo "Unzipping Kafka..."
tar -xzf $TEMP_FILE -C $DEST_DIR

# Rename the Kafka directory to kafka-client
mv $DEST_DIR/kafka_2.13-3.4.1 $DEST_DIR/kafka-client

# Clean up the temporary file
rm $TEMP_FILE

echo "Kafka has been downloaded, unzipped, and renamed as kafka-client in $DEST_DIR"
