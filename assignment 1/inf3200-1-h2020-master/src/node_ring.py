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

from http.server import BaseHTTPRequestHandler,HTTPServer


object_store = {} 
neighbors = [] 
checked_key = "" # variable for whether the key was previously checked

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

        object_store[key] = value 
        
        self.send_whole_response(200, "Value stored for " + key) 

    def do_GET(self): 
        global checked_key 
        if self.path.startswith("/storage"): 
            key = self.extract_key_from_path(self.path) 
            
            if checked_key!=key: #if the received key is not the one you checked before,
                checked_key = key
                if key in object_store: 
                    self.send_whole_response(200, object_store[key]) 
                else: 
                    for neighbor in neighbors:
                        returned = self.get_value(neighbor, key) 
                        if returned == None: #if returned is None (if the key is not found on neighbor I requested)
                            self.send_whole_response(404, "No object with key '%s' on this node" % key)  
                        else: 
                            if type(returned)!=bytes:  
                                returned=returned.encode()
                            self.send_whole_response(200, returned)
                        
            else: #if the received key is the one you checked before,
                self.send_whole_response(404, "No object with key '%s' on this node" % key) 


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

def run_server(args): 
    global server
    global neighbors
    server = ThreadingHttpServer(('', args.port), NodeHttpHandler)
    neighbors = args.neighbors

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
    print(args) 
    run_server(args)
