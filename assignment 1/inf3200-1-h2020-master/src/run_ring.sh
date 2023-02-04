#!/bin/sh


if [ $# -lt 1 ]; then
    echo "Usage: num_hosts"
    exit 
fi

echo "run $1 servers"
/share/apps/ifi/available-nodes.sh | shuf | head -n $1 > hosts

cat hosts | sort -R > sortedHosts

readarray -t hostsArray < sortedHosts 

for ((i=0; i < ${#hostsArray[@]}; ++i)); do
    if (( $i >= ${#hostsArray[@]}-1)); then
        ssh -f ${hostsArray[$i]} python3 $(pwd)/node_ring.py -p 9000 ${hostsArray[0]}:9000 > /dev/null
    else
        ssh -f ${hostsArray[$i]} python3 $(pwd)/node_ring.py -p 9000 ${hostsArray[$i+1]}:9000 > /dev/null
    fi
done