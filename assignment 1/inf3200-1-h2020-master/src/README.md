### To run a simple ring based chord system
$ sh run_ring.sh {number of nodes}
### To run a finger table based chord system
$ sh run.sh {number of nodes}  
### To test interaction with the nodes:
$ python3 client.py {hostname}:{port}
Where {hostname} is the local hostname of any of the nodes in the Chord system. A list is generated in either the "sortedHosts" or "hosts" file.
