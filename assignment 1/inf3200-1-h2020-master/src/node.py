#!/usr/bin/env python3
import argparse
import json
import re
import signal
import socket
import socketserver
import threading

import os
import http.client
import hashlib # For consistent hashing with SHA1
from math import log
from http.server import BaseHTTPRequestHandler,HTTPServer

object_store = {} 
neighbors = [] 

class Chord():
    def __init__(self, hostname, key_size):
        self.hostname = hostname
        self.key_size = key_size
        # Open the sortedHosts file and put file contents into a string
        # Then turn the file contents into a Python list
        unsorted_hosts_list = open(
            (os.path.dirname(os.path.abspath(__file__)) + "/sortedHosts"), "r"
        ).read().split("\n")
        unsorted_hosts_list.pop()

        hosts_list = list()
        for host in unsorted_hosts_list:
            sortval = hash_fn(host, self.key_size)
            hosts_list.append((sortval, host))
        hosts_list.sort()

        # Get the length of that list
        self.hosts_num = len(hosts_list)
        # Get your hash id
        self.hash_id = hash_fn(self.hostname, self.key_size)
        # Get your index in the Chord ring
        self.index = 0
        for node in hosts_list:
            if node[1] != self.hostname:
                self.index += 1
            else:
                break

        self.finger_table = list()
        self.finger_table_length = int(log(self.hosts_num,2))

        # Finger table entries are structured as such:
        # hash_id | hostname | key_range_start | key_range_end
        # The first entry is this node's successor
        for i in range(self.finger_table_length):
            ft_entry = {
                "hash_id": hosts_list[(self.index + 2**i) % self.hosts_num][0],
                "hostname": hosts_list[(self.index + 2**i) % self.hosts_num][1],
                "range_start": hosts_list[(self.index + 2**i) % self.hosts_num][0], 
                "range_end": -1
            }
            self.finger_table.append(ft_entry)

        # Determine distances for yourself and your fingers
        # Range start is same as hash_id
        self.range_start = self.hash_id
        if len(self.finger_table)==0: ###
            self.range_end = self.hash_id-1 ### 
        else: ### 
            self.range_end = self.finger_table[0]["hash_id"]-1 ###

        for i in range(self.finger_table_length):
            # Exception: Last entry in finger table, i+1 does not exist
            if i == self.finger_table_length-1:
                self.finger_table[i]["range_end"] = self.hash_id-1
            else:
                # Exception: i's +1 is at exactly hashID 0, and 0-1 = -1 which is an invalid value.
                if self.finger_table[i+1]["hash_id"] == 0:
                    self.finger_table[i]["range_end"] = self.key_size
                else:
                    self.finger_table[i]["range_end"] = self.finger_table[i+1]["range_start"]-1

    # Checks the given key if it is in the range of the given entry.
    # -1 means self.
    def check_key(self, key, entry):

        # Check if entry is -1 (start and end of range is stored in self)
        if entry == -1:
            r_start = self.range_start
            r_end = self.range_end
        # Check if entry is invalid
        elif entry > self.finger_table_length:
            return False
        # If not, entry is in our finger table. Set start and end.
        else:
            r_start = self.finger_table[entry]["range_start"]
            r_end=self.finger_table[entry]["range_end"]

        # Determine how to check the range in which key is in
        # check if start is greater than end (ex. 21315 - 4431)
        if r_start > r_end:
            if key not in range (r_end, r_start):
                return True
        # check if start is equal to end
        elif (r_start == r_end):
            if (r_start == key):
                return True
        # If not, start is smaller than end (ex. 12345 - 22242)
        else:
            if key in range (r_start, r_end):
                return True
        return False



class NodeHttpHandler(BaseHTTPRequestHandler):
    def send_whole_response(self, code, content, content_type="text/plain"): 
        if isinstance(content, str): 
            content = content.encode("utf-8")  
            if not content_type: 
                content_type = "text/plain" 
            if content_type.startswith("text/"):  
                content_type += "; charset=utf-8"  
        elif isinstance(content, bytes): 
            if not content_type:
                content_type = "application/octet-stream" 
        elif isinstance(content, object): 
            content = json.dumps(content, indent=2) 
            content += "\n" 
            content = content.encode("utf-8") 
            content_type = "application/json" 

        self.send_response(code) 
        self.send_header('Content-type', content_type) 
        self.send_header('Content-length',len(content)) 
        self.end_headers() 
        self.wfile.write(content) 

    def extract_key_from_path(self, path): 
        return re.sub(r'/storage/?(\w+)', r'\1', path) 

    def do_PUT(self): 
        content_length = int(self.headers.get('content-length', 0)) 

        key = self.extract_key_from_path(self.path) 
        value = self.rfile.read(content_length) 

        # Hash the given key
        hashed_key = hash_fn(key, chord.key_size)
        if (chord.check_key(hashed_key, -1) == True):
            object_store[key] = value

        else:
            for x in reversed(range(chord.finger_table_length)):
                if(chord.check_key(hashed_key, x)):
                    self.put_value("{}:9000".format(chord.finger_table[x]["hostname"]), key, value) 
                    break
        
        self.send_whole_response(200, "Value stored for " + key) 

    def do_GET(self): 
        if self.path.startswith("/storage"): 
            key = self.extract_key_from_path(self.path) 
            hashed_key = hash_fn(key, chord.key_size)

            if key in object_store:
                self.send_whole_response(200, object_store[key]) 
            # If key was not found in object_store but we ARE responsible, it's not in the system.
            elif chord.check_key(hashed_key, -1):
                self.send_whole_response(404, "No object with key '%s' on this node" % key) 
            # Send down to a friend
            else:
                for x in reversed(range(chord.finger_table_length)):
                    if (chord.check_key(hashed_key, x)):
                        returned = self.get_value("{}:9000".format(chord.finger_table[x]["hostname"]), key)
                        # Guess they didn't find it either :(
                        if returned == None:
                            self.send_whole_response(404, "No object with key '%s' on this node" % key)
                        else:
                            if type(returned)!=bytes:
                                returned=returned.encode()
                            self.send_whole_response(200, returned)
                        break

        elif self.path.startswith("/neighbors"): 
            self.send_whole_response(200, neighbors)  

        else: 
            self.send_whole_response(404, "Unknown path: " + self.path) 

        
    def get_value(self, node, key): 
        conn = http.client.HTTPConnection(node) 
        conn.request("GET", "/storage/"+key) 
        resp = conn.getresponse() 
        headers = resp.getheaders() 
        if resp.status != 200:  
            value = None 
        else: 
            value = resp.read() 
        contenttype = "text/plain" 
        for h, hv in headers: 
            if h=="Content-type": 
                contenttype = hv 
        if contenttype == "text/plain": 
            if value != None:
                value = value.decode("utf-8") 
        conn.close() 
        return value 

    def put_value(self, node, key, value):
        conn = http.client.HTTPConnection(node)
        conn.request("PUT", "/storage/"+key, value)
        conn.getresponse()
        conn.close()


def arg_parser():
    PORT_DEFAULT = 8000 
    DIE_AFTER_SECONDS_DEFAULT = 20 * 60

    parser = argparse.ArgumentParser(prog="node", description="DHT Node")

    parser.add_argument("-p", "--port", type=int, default=PORT_DEFAULT,
            help="port number to listen on, default %d" % PORT_DEFAULT)

    parser.add_argument("--die-after-seconds", type=float,
            default=DIE_AFTER_SECONDS_DEFAULT,
            help="kill server after so many seconds have elapsed, " +
                "in case we forget or fail to kill it, " +
                "default %d (%d minutes)" % (DIE_AFTER_SECONDS_DEFAULT, DIE_AFTER_SECONDS_DEFAULT/60))

    parser.add_argument("neighbors", type=str, nargs="*",
            help="addresses (host:port) of neighbour nodes")

    return parser 

class ThreadingHttpServer(socketserver.ThreadingMixIn, HTTPServer):
    pass

# https://levelup.gitconnected.com/consistent-hashing-27636286a8a9
def hash_fn(key, modulo):
    hasher = hashlib.sha1()
    hasher.update(bytes(key.encode("utf-8")))
    return int(hasher.hexdigest(), 16) % modulo

# https://www.linuxjournal.com/article/6797
#def ring_distance(a, b):
#    if a == b:
#        return 0
#    elif a < b:
#        return b-a
#    else:
#        return(2**160)+(b-a)

def run_server(args): 
    global server
    global neighbors
    global chord # Hacky but might let us do what we need to do

    server = ThreadingHttpServer(('', args.port), NodeHttpHandler)

    hostname = server.server_name.split(".", 1)[0]

    chord = Chord(hostname, 65536) # Set high enough to make collisions improbable

    neighbors.clear()
    for finger in chord.finger_table:
        neighbors.append("{}:9000".format(finger["hostname"]))
    

    def server_main(): 
        server.serve_forever() 
        print("Server has shut down") 

    def shutdown_server_on_signal(signum, frame): 
        print("We get signal (%s). Asking server to shut down" % signum) 
        server.shutdown()

    thread = threading.Thread(target=server_main) 
    
    thread.daemon = True 

    thread.start() 

    signal.signal(signal.SIGTERM, shutdown_server_on_signal) 
    signal.signal(signal.SIGINT, shutdown_server_on_signal) 

    thread.join(args.die_after_seconds) 

    if thread.is_alive(): 
        print("Reached %.3f second timeout. Asking server to shut down" % args.die_after_seconds) 
        server.shutdown() 

    print("Exited cleanly") 

if __name__ == "__main__":

    parser = arg_parser() 
    args = parser.parse_args() 
    run_server(args)
