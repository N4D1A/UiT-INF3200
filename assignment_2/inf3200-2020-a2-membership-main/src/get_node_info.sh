#!/bin/sh
readarray -t hostsPortArray < hosts

for ((i=0; i < ${#hostsPortArray[@]}; ++i)); do
    curl -X GET "${hostsPortArray[$i]}/node-info"
done