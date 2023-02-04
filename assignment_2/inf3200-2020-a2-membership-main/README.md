Assignment Handout for UiT INF-3200 Assignment 2, Fall 2020
==================================================

This is assignment hand-out text and example code for assignment 2
of the Distributed Systems course (INF-3200) at UiT the Arctic University of Norway.

- See [handout-text/inf3200-a2-membership.pdf](handout-text/inf3200-a2-membership.pdf) for assignment details.
- See the [example-code/](example-code/) directory for starter code.  

Source code and scripts can be found in the src folder. Once there:  
### To initialize a simple ring based Chord network with n nodes:
$ sh start_network_n.sh n

### To initialize n number of nodes which are not connected to any other nodes:
$ sh start_singles_n.sh n

### To run an API check on an existing network:
$ sh api_check.sh

### To run experiments on an existing cluster of nodes which are not joined in a network (bash wrapper for experiments.py):
$ sh run_experiments.sh

### To open htop only showing processes which belong to the current user:
$ sh list_own_processes.sh

### To prematurely terminate all nodes which have been started with either the start_singles_n.sh or start_network_n.sh scripts:
$ sh kill_nodes.sh

### To get the info of all nodes which has been booted up regardless of network status:
$ sh get_node_info.sh