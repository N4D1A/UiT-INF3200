#!/usr/bin/env python3

import argparse
import json
import re
import random
import threading
import string
import time
import unittest
import uuid

# Logger
import logging
logging.basicConfig()
logger = logging.getLogger()

# Python version check
import sys

import http.client as httplib

SETTLE_MS_DEFAULT = 40
settle_ms = SETTLE_MS_DEFAULT

def set_test_nodes(nodes):
    global test_nodes
    test_nodes = nodes

def parse_args():
    parser = argparse.ArgumentParser(prog="api_check", description="node API checker")

    parser.add_argument("--settle-ms", type=int,
            default=SETTLE_MS_DEFAULT,
            help="After a join/leave call, wait for the network to settle (default {} ms)"
                .format(SETTLE_MS_DEFAULT))

    parser.add_argument("nodes", type=str, nargs="*",
            help="addresses (host:port) of nodes to test")

    return parser.parse_args()

def describe_exception(e):
    return "%s: %s" % (type(e).__name__, e)

class Response(object): pass

def search_header_tuple(headers, header_name):
    header_name = header_name.lower()

    for key, value in headers:
        if key.lower() == header_name:
            return value
    return None

def determine_charset(content_type):
    cmatch = re.match("text/plain; ?charset=(\\S*)", content_type)
    if cmatch:
        return cmatch.group(1)
    else:
        return "latin_1"

def do_request(host_port, method, url, body=None, accept_statuses=[200]):
    def describe_request():
        return "%s %s%s" % (method, host_port, url)

    conn = None
    try:
        conn = httplib.HTTPConnection(host_port)
        try:
            conn.request(method, url, body)
            r = conn.getresponse()
        except Exception as e:
            raise Exception(describe_request()
                    + " --- "
                    + describe_exception(e))

        status = r.status
        if status not in accept_statuses:
            raise Exception(describe_request() + " --- unexpected status %d" % (r.status))

        headers = r.getheaders()
        body = r.read()

    finally:
        if conn:
            conn.close()

    content_type = search_header_tuple(headers, "Content-type")

    if content_type == "application/json":
        try:
            body = json.loads(body)
        except Exception as e:
            raise Exception(describe_request()
                    + " --- "
                    + describe_exception(e)
                    + " --- Body start: "
                    + body[:30])

    if content_type != None and content_type.startswith("text/plain") \
            and sys.version_info[0] >= 3:
        charset = determine_charset(content_type)
        body = body.decode(charset)

    r2 = Response()
    r2.status = status
    r2.headers = headers
    r2.body = body

    return r2



if __name__ == "__main__":

    args = parse_args()

    test_nodes = args.nodes

    if len(test_nodes) < 2:
        print("Must have at least two nodes")
        exit

    t1_time = t2_time = t3_time = 0
    
    # We assume the network is already set up and accepting connections.
    # We assume the network is not in a ring.

    # Test 1: Time to grow network to 50 nodes.
    # Join them all together. This is when we start the clock.
    print("Starting test 1...")
    start = time.time()
    for node in test_nodes:
        # Yes, we are asking node 0 to join node 0. 
        # Is it working? Yes. Do we know why? Not really, no.
        r = do_request(node, "POST", "/join?nprime="+test_nodes[0])
    stop = time.time()

    for node in test_nodes:
        r = do_request(node, "GET", "/node-info")
        print(r.body)
    
    t1_time = stop-start
    print("Done.\nTime test 1: {}".format(t1_time))

    # Test 2: Time to shrink a network down to 25 nodes.
    # This test needs 50 nodes to start with, so if there aren't 50 nodes, we skip it.
    if len(test_nodes) >= 50:
        start = time.time()
        for num in range(10,35):
            r = do_request(test_nodes[num], "POST", "/leave")
        stop = time.time()
        t2_time = stop-start
        print("Done test 2.\nTime: {}".format(t2_time))
    else:
        print("Not enough nodes for test 2, skipping.")
    
    # Test 3: Crash resilliance
    if len(test_nodes) >= 2:
        start = time.time()
        crashed_nodes = 0
        nodes_to_crash = 1
        while True:
            if ((crashed_nodes + nodes_to_crash) >= len(test_nodes) ):
                break
            for num in range(nodes_to_crash):
                r = do_request(test_nodes[crashed_nodes], "POST", "/sim-crash")
                crashed_nodes += 1
            nodes_to_crash += 1
        stop = time.time()
        t3_time = stop-start
        print("Test 3: Crashed {} nodes total".format(crashed_nodes))
        print("Time taken for test 3: {}".format(t3_time))

    else:
        print("Not enough nodes for test 3, skipping.")

    print("\n\nSummary:")
    print("Number of nodes: {}".format(len(test_nodes)))

    print("Test 1 time elapsed: {}".format(t1_time))
    if t2_time == 0:
        print("Test 2 time elapsed: DNF")
    else: 
        print("Test 2 time elapsed: {}".format(t2_time))

    if t3_time == 0:
        print("Test 3 time elapsed: DNF")
    else: 
        print("Test 3 time elapsed: {}".format(t3_time))
        print("Number of nodes sim-crashed: {}".format(crashed_nodes))

