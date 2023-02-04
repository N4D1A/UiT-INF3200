#!/bin/sh
# Get list of all clusters regardless of availability, pipe into cluster_hostnames_list.txt
/share/apps/ifi/list-cluster-static.sh | grep -o "compute-\w*-\w*" > cluster_hostnames_list.txt
# Use VSCode with regular expressions to pre-format for the python code.