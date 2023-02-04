#!/bin/sh
# Format the hosts list to be separated by spaces instead of newlines
tr '\n' ' ' < hosts > spacedHosts
# Read the new file into a value
read valueForPython < spacedHosts
# Run the api check script
python3 api_check.py $valueForPython
# remove the temp file
rm spacedHosts