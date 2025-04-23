#!/bin/bash

# Create the data directory if it doesn't exist
mkdir -p ./data

# Download the ZIP file into the data directory
curl -L -o ./data/us-accidents.zip \
  https://www.kaggle.com/api/v1/datasets/download/sobhanmoosavi/us-accidents

# Unzip the file into the data directory
unzip ./data/us-accidents.zip -d ./data

# Delete the ZIP file
rm ./data/us-accidents.zip
