#!/bin/bash

# Get the instrument name from the first argument (required)
INSTRUMENT=$1

# Check if instrument is provided
if [ -z "$INSTRUMENT" ]; then
    echo "Error: Instrument name must be specified"
    exit 1
fi

# Source conda initialization
. /home/essdaq/miniconda3/etc/profile.d/conda.sh
# Activate the beamlime environment
conda activate beamlime

# Change to the beamlime directory
cd /home/essdaq/essproj/beamlime

# Execute the Python module with arguments
python -m beamlime.services.detector_data --instrument $INSTRUMENT --log=DEBUG
