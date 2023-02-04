#!/bin/sh
# Format the hosts list to be separated by spaces instead of newlines
tr '\n' ' ' < hosts > spacedHosts
# Read the new file into a value
read valueForPython < spacedHosts
# Run the experiments python script
python3 experiments.py $valueForPython
# remove the temp file
rm spacedHosts