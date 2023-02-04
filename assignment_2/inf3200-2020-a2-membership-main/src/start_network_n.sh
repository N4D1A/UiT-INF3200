#!/bin/sh

if [ $# -lt 1 ]; then
    echo "Usage: num_hosts"
    exit 
fi

echo "run $1 servers"
/share/apps/ifi/available-nodes.sh | grep compute | shuf | head -n $1 > hostsNoPort

awk '{print $0":55555"}' hostsNoPort > hosts

readarray -t hostsNoPortArray < hostsNoPort
readarray -t hostsPortArray < hosts

for ((i=0; i < ${#hostsNoPortArray[@]}; ++i)); do
    ssh -f ${hostsNoPortArray[$i]} python3 $(pwd)/node.py -p 55555
done

sleep 2

for ((i=0; i < ${#hostsPortArray[@]}; ++i)); do
    curl -X POST "${hostsPortArray[$i]}/join?nprime=${hostsPortArray[0]}"
done