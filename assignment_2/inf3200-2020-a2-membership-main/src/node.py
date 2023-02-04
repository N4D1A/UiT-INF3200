#!/usr/bin/env python3
import argparse
import json
import re
import signal
import socket
import socketserver
import threading

from http.server import BaseHTTPRequestHandler,HTTPServer
import hashlib # For consistent hashing with SHA1
import datetime ## To get the current time for logging purposes
import http.client
from math import log
import time
import os

# Logger
import logging
import pickle
import subprocess

# Assignment 1 node properties
object_store = {}
neighbors = [] 

# Assignment 2 node properties
node_name = None
node_key = None
successor = None
other_neighbors = []
sim_crash = False
predecessor = None ##
neighbors_set = set() ## 

my_index = None ##
node_range_start = None ##
node_range_end = None ##

stop_requested = False ##
key_size = 2**16
            
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

    def is_responsible_for_key(self, key):
        global node_key
        global successor
        hashed_key = hash_fn(key, key_size)
        successor_key = hash_fn(successor, key_size)
        if node_key > successor_key:
            if hashed_key not in range(successor_key, node_key):
                return True
            else:
                return False
        elif node_key < successor_key:
            if hashed_key in range(node_key, successor_key):
                return True
            else:
                return False
        elif node_key == successor_key:
            return True

    def do_PUT(self):
        global node_key
        global node_name
        global successor
        global predecessor
        global other_neighbors
        global sim_crash
        global neighbors_set
        
        if sim_crash == True: ## 만약 sim_crash가 True면 ###
            self.send_whole_response(500, "I have sim-crashed") ## sim_crashed 되었다고 송출 ###
            return #####
                
        ##### PUT
        #  요청받은 노드와 요청받은 내용 로깅
        logging.info("Received PUT request from {}:\n{}".format(self.client_address[0], self.requestline)) ##
        content_length = int(self.headers.get('content-length', 0))

        key = self.extract_key_from_path(self.path) ### extract_key_from_path()는 /storage/ 경로만 삭제함.
        value = self.rfile.read(content_length)

        logging.debug("node_key is {}".format(node_key)) ##
        logging.debug("successor_key is {}".format(hash_fn(successor, key_size))) ##

        # Hash the given key
        hashed_key = hash_fn(key, key_size)
        logging.debug("hashed_key is {}".format(hashed_key)) ## 해쉬 키값 로깅

        if self.is_responsible_for_key(key):
            logging.info("PUT: I have responsibility for the key {} in my range {} from {}!".format(hashed_key, node_key, hash_fn(successor, key_size)-1)) ##
            object_store[key] = value  ## 자기노드의 store에 저장
        else:
            logging.info("PUT: I'm redirecting the key {} to my neighbor {}!".format(hashed_key, successor)) ##
            self.put_value(successor, key, value)
            
        # Send OK response
        self.send_whole_response(200, "Value stored for " + key) ## 키 저장했다고 송출


    def do_GET(self):
        global node_key
        global node_name
        global successor
        global predecessor
        global other_neighbors
        global sim_crash
        global stop_requested ##
        global neighbors_set
        
        ##### GET 요청받은 노드와 요청받은 내용 로깅
        logging.info("Received GET request from client_address[0]:{}:\n{}".format(self.client_address[0], self.requestline)) ##
        
        ## 받은 요청이 /node-info로 시작하면 딕셔너리 형태로 저장된 노드 정보를 json으로 반환
        if self.path == "/node-info":
            node_info = { ## node_info에 다음 속성 값들을 딕셔너리 형태로 저장
                    "node_key": node_key,
                    "successor": successor,
                    "predecessor": predecessor,
                    "others": other_neighbors,
                    "neighbors" : neighbors,
                    "sim_crash": sim_crash
                    }
            node_info_json = json.dumps(node_info, indent=2) # json 형태로 변환
            self.send_whole_response(200, node_info_json, content_type="application/json") ## json 값 송출

        elif sim_crash == True: ## 만약 sim_crash가 True면
            self.send_whole_response(500, "I have sim-crashed") ## sim_crashed 되었다고 송출
            return #####

        elif self.path.startswith("/storage/"): ## 받은 요청이 /storage로 시작하면
            key = self.extract_key_from_path(self.path) ## 키 추출해서 해쉬 키로 바꿈
            hashed_key = hash_fn(key, key_size)
            logging.debug("GET: hashed_key is {}".format(hashed_key)) ## 해쉬 키값 로깅

            if key in object_store:
                logging.info("GET: I have responsibility for the key {}:{} in my range {} from {}!".format(key, hashed_key, node_key, hash_fn(successor, key_size)-1)) ##
                self.send_whole_response(200, object_store[key])

            elif self.is_responsible_for_key(key):
                logging.info("GET: I have responsibility for the key {}. but no object with key {} on this node".format(key, hashed_key)) ##
                self.send_whole_response(404, "No object with key '%s' on this node" % key)

            else:
                logging.info("GET: I'm redirecting the key {}:{} to my successor {}!".format(key, hashed_key, successor)) ##
                returned = self.get_value(successor, key)
                if returned == None: #if returned is None (if the key is not found on neighbor I requested)
                    self.send_whole_response(404, "No object with key '%s' on this node" % key)
                else:
                    if type(returned)!=bytes:
                        returned=returned.encode()
                    self.send_whole_response(200, returned)

        ## 받은 요청이 /neighbors로 시작하면 이웃 리스트 반환
        elif self.path.startswith("/neighbors"):
            logging.debug("neighbors is {}".format(neighbors))
            self.send_whole_response(200, neighbors)

        ## 받은 요청이 /node_name로 시작하면 node_name 반환
        elif self.path.startswith("/node_name"):
            self.send_whole_response(200, node_name)

        ## 받은 요청이 /node_key로 시작하면 node_key 반환
        elif self.path.startswith("/node_key"):
            self.send_whole_response(200, node_key)

        ## 받은 요청이 /successor로 시작하면 successor 반환
        elif self.path.startswith("/successor"):
            self.send_whole_response(200, successor)

        ## 받은 요청이 /predecessor로 시작하면 predecessor 반환
        elif self.path.startswith("/predecessor"):
            self.send_whole_response(200, predecessor)

        ## 받은 요청이 /other_neighbors로 시작하면 other_neighbors 반환
        elif self.path.startswith("/other_neighbors"):
            self.send_whole_response(200, other_neighbors)

        ## 받은 요청이 /ask_join로 시작하면 조인할 위치의 후임자와 선임자 반환
        elif self.path.startswith("/ask_join/"):
            logging.debug("in /ask_join/")
            asked_node = re.sub(r'/ask_join/?(\w+)', r'\1', self.path)
            hashed_key = hash_fn(asked_node, key_size) ## 키 추출해서 해쉬 키로 바꿈
            logging.debug("asked_node is {}, it's hashed_key is {}".format(asked_node, hashed_key))
            if self.is_responsible_for_key(asked_node):
                logging.debug("1, my responsibility")

                b_response = json.dumps([successor, node_name]) ### string to bytes (1)
                
                returned = b_response # bytes of [successor, predecessor]
                logging.debug("responsible node(me):{}, returned  is {}".format(node_name, returned))

                if returned == None: #if returned is None
                    self.send_whole_response(404, "Error '%s'" % asked_node) # send error msg
                    logging.debug("returned Error with 404")

                else:
                    self.send_whole_response(200, returned) # send returned (bytes of [successor, predecessor])
                    logging.debug("returned {} with 200".format(returned))

                ## 자기영역에 새 노드가 들어올 경우만 아래의 선임, 후임 교체 실행
                self.notify_predecessor(asked_node, successor) ### (3) (go down)
                logging.debug("Successor Updated!!!: from {} to {}".format(successor, asked_node))
                successor = asked_node ### (4)
                other_neighbors = self.get_successor(successor) ## 후임의 후임 저장
            else:
                logging.debug("2, redirecting")
                returned = self.ask_join(asked_node, successor) # bytes of [successor, predecessor] (1)
                logging.debug("responsible node:{}, returned is {}".format(successor, returned))
                
                if returned == None: #if returned is None
                    self.send_whole_response(404, "Error '%s'" % asked_node) # send error msg
                    logging.debug("returned Error with 404")

                else:
                    self.send_whole_response(200, returned) # send returned (bytes of [successor, predecessor])
                    logging.debug("returned {} with 200".format(returned))
            
                                
        ## 받은 요청이 /notify_predecessor로 시작하면 나의 선임자를 전달받은 요청노드로 변경
        elif self.path.startswith("/notify_predecessor/"):
            logging.debug("in /notify_predecessor/")
            new_joined_node = re.sub(r'/notify_predecessor/?(\w+)', r'\1', self.path)
            logging.debug("Predecessor Updated!!: from {} to {}".format(predecessor, new_joined_node))
            predecessor = new_joined_node ### (3) 실행됨
            
        ## 받은 요청이 /notify_successor로 시작하면 나의 선임자를 전달받은 요청노드로 변경
        elif self.path.startswith("/notify_successor/"):
            logging.debug("in /notify_successor/")
            new_node = re.sub(r'/notify_successor/?(\w+)', r'\1', self.path)
            logging.debug("Successor Updated!!: from {} to {}".format(successor, new_node))
            successor = new_node ### (3) 실행됨
            other_neighbors = self.get_successor(successor) ## 후임의 후임 저장
            
        ## 받은 요청이 /stop_requested로 시작하면 stop_requested 설정 ###
        elif self.path.startswith("/stop_requested"):
            if stop_requested==True:
                stop_requested=False
            else:
                stop_requested=True
            self.send_whole_response(200, stop_requested)

        else:
            self.send_whole_response(404, "Unknown path: " + self.path)

    def do_POST(self):
        global node_key
        global node_name
        global successor
        global predecessor
        global other_neighbors
        global sim_crash
        global neighbors_set

        if self.path == "/sim-recover":
            sim_crash = False
            self.send_whole_response(200, "")

        elif self.path == "/sim-crash":
            sim_crash = True
            self.send_whole_response(200, "")

        elif sim_crash == True:
            self.send_whole_response(500, "I have sim-crashed")
            return #####

        elif self.path == "/leave":
            self.notify_successor(successor, predecessor) # 선임에게 너의 새로운 후임으로 내 후임 알려줌
            self.notify_predecessor(predecessor, successor) # 후임에게 너의 새로운 선임으로 내 선임 알려줌

            successor = node_name # 후임은 나로 재설정
            other_neighbors = node_name ## 후임의 후임 저장
            predecessor = node_name # 선임은 나로 재설정
            self.send_whole_response(200, "")

        elif self.path.startswith("/join"):
            nprime = re.sub(r'^/join\?nprime=([\w:-]+)$', r'\1', self.path)
            logging.debug("in /join\?nprime= nprime:{}".format(nprime))
            b_response = self.ask_join(node_name, nprime)
            response = json.loads(b_response) 
            logging.debug("Successor updated! from {} to {}".format(successor, response[0]))
            successor = response[0] ### (2)
            other_neighbors = self.get_successor(successor) ## 후임의 후임 지정
            logging.debug("Predecessor updated! from {} to {}".format(predecessor, response[1]))
            predecessor = response[1] ### (2)
            
            self.send_whole_response(200, response)

        else:
            self.send_whole_response(404, "Unknown path: " + self.path)


    def ask_join(self, new_node_to_join, node_in_network):
        logging.debug("in self.ask_join()") ##
        conn = http.client.HTTPConnection(node_in_network)
        conn.request("GET", "/ask_join/"+new_node_to_join) # path: "/ask_join/"+new_node_to_join
        logging.debug("HTTPConnection to {}: /ask_join/{}".format(node_in_network, new_node_to_join)) ##
                
        resp = conn.getresponse()
        headers = resp.getheaders()
        if resp.status != 200:
            value = None
        else:
            value = resp.read()
            value = value.decode("utf-8")
        conn.close()
        return value

    def get_successor(self, node):
        conn = http.client.HTTPConnection(node) ## 받은 노드와 연결해 instance 생성
        conn.request("GET", "/successor")
        resp = conn.getresponse()
        headers = resp.getheaders() ## 헤더 읽어서
        if resp.status == 500:  ## 500이면 다음노드는 크래쉬된 것
            
            value = None
        else:
            value = resp.read() ## 200이면 value에 받은 값 저장
        contenttype = "text/plain"
        for h, hv in headers:
            if h=="Content-type":
                contenttype = hv
        if contenttype == "text/plain":
            if value != None:
                value = value.decode("utf-8")
        conn.close()
        return value

    def notify_predecessor(self, new_node_joined, my_successor): # 후임에게 너의 새로운 선임 알려줌
        global node_name
        global successor
        global predecessor
        global neighbors
        global neighbors_set
        logging.debug("in self.notify_predecessor()") ##
        if my_successor == node_name:
            logging.debug("my_successor: {} == node_name: {}".format(my_successor, node_name)) ##
            logging.debug("Predecessor updated!: from {} to {}".format(predecessor, new_node_joined)) ##
            predecessor = new_node_joined
        else:
            logging.debug("my_successor: {}, node_name: {}".format(my_successor, node_name)) ##
            conn = http.client.HTTPConnection(my_successor)
            conn.request("GET", "/notify_predecessor/"+new_node_joined)
            logging.debug("HTTPConnection to {}: /notify_predecessor/{}".format(my_successor, new_node_joined)) ##

    def notify_successor(self, new_node, my_predecessor): # 선임에게 너의 새로운 후임 알려줌
        global node_name
        global successor
        global predecessor
        global neighbors
        global neighbors_set
        logging.debug("in self.notify_successor()") ##
        if my_predecessor == node_name:
            logging.debug("my_predecessor: {} == node_name: {}".format(my_predecessor, node_name)) ##
            logging.debug("Successor updated!: from {} to {}".format(successor, new_node)) ##
            successor = new_node
            other_neighbors = self.get_successor(successor) ## 후임의 후임 지정
        else:
            logging.debug("my_predecessor: {}, node_name: {}".format(my_predecessor, node_name)) ##
            conn = http.client.HTTPConnection(my_predecessor)
            conn.request("GET", "/notify_successor/"+new_node)
            logging.debug("HTTPConnection to {}: /notify_successor/{}".format(my_predecessor, new_node)) ##
            
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
        conn = http.client.HTTPConnection(node) ## 받은 노드와 연결해 instance 생성
        conn.request("PUT", "/storage/"+key, value) ## instance에 키/밸류 저장 요청 ("PUT"이 'do_PUT'임)
        conn.getresponse() ## 응답 받고
        conn.close() ## 닫기

def arg_parser():
    PORT_DEFAULT = 8000
    DIE_AFTER_SECONDS_DEFAULT = 20 * 60

    LOGGER_DEFAULT = logging.DEBUG ##
    LOGGER_PATH_DEFAULT = "{}/log".format(os.path.dirname(os.path.abspath(__file__))) ##

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

    parser.add_argument("-d", "--debug", type=int, ##
            default=LOGGER_DEFAULT,
            help="what level of logging to be enabled. Valid options are " +
                "0 (None), 10 (DEBUG), 20 (INFO), 30 (WARNING), 40 (ERROR) " +
                "and 50 (CRITICAL). Default is 0 (None).")

    parser.add_argument("-dp", "--debug-path", type=str, ##
            default=LOGGER_PATH_DEFAULT,
            help="the path to the folder where log files will end up." +
                "Default is a log folder in the same directory as the script.")

    return parser

class ThreadingHttpServer(socketserver.ThreadingMixIn, HTTPServer):
    pass

def hash_fn(key, modulo):
    hasher = hashlib.sha1()
    hasher.update(bytes(key.encode("utf-8")))
    return int(hasher.hexdigest(), 16) % modulo

def run_server(args):
    global server
    global neighbors
    
    global node_name
    global node_key
    global successor
    global other_neighbors
    global sim_crash
    global predecessor ###
    global neighbors_set

    global myIndex ##

    server = ThreadingHttpServer(('', args.port), NodeHttpHandler)

    if ((os.path.lexists(args.debug_path) == False) and (args.debug != logging.NOTSET)): ##
        os.mkdir(args.debug_path) ##
    now = datetime.datetime.now() ##
    log_time=("%02d:%02d:%02d"%(now.hour, now.minute, now.second)) ##

    node_name = server.server_name.split(".", 1)[0]
    node_name = f"{node_name}:{args.port}" #####
    node_key = hash_fn(node_name, key_size)

    logging.basicConfig(filename="{}/{}_{}.log".format(args.debug_path, node_name, log_time), ###
        format="%(relativeCreated)6d %(levelname)s: %(message)s", level=args.debug) ###
    global logger
    logger = logging.getLogger() ###

    logger.info("Logging set to level {}".format(args.debug))
    logger.info("Server name is {}".format(node_name))
    logger.info("Timeout set to {} seconds".format(args.die_after_seconds))

    logger = logging.getLogger(node_name)
    logger.setLevel(logging.INFO)

    ## 호스트리스트 정렬하기
    unsorted_hosts_list = open(
        (os.path.dirname(os.path.abspath(__file__)) + "/hosts"), "r"
    ).read().split("\n")
    unsorted_hosts_list.pop()

    hosts_list = list()
    for host in unsorted_hosts_list:
        sortval = hash_fn(host, 2**16)
        hosts_list.append((sortval, host))
    hosts_list.sort()

    ##### 정렬된 호스트리스트 로깅
    logging.info("Sorted node list is:") ##
    for sorted_node in hosts_list: ##
        logging.info(sorted_node) ##

    hosts_num = len(hosts_list)
    my_index = 0
    for node in hosts_list:
         if node[1] != node_name: # node[0] = node_key
            my_index += 1
        else:
            break

    logging.info("my index: {}".format(my_index)) ##
    logging.info("my successor: {}".format(successor)) ##

    if len(args.neighbors) == 0:
        successor = node_name
        predecessor = node_name ###
        other_neighbors = node_name

    if len(args.neighbors) >= 1:
        successor = hosts_list[(my_index+1) % hosts_num][1] ##
        predecessor = args.neighbors[-1] ###
        
        other_neighbors = args.neighbors[1:] ## 

    logging.info("my successor: {}".format(successor)) ##
    logging.info("my predecessor: {}".format(predecessor)) ##

    neighbors = args.neighbors


    def server_main():
        logger.info("Starting server on port %d" , args.port)
        server.serve_forever()
        logger.info("Server has shut down")

    def shutdown_server_on_signal(signum, frame):
        logger.info("We get signal (%s). Asking server to shut down", signum)
        server.shutdown()

    def stabilization(): 
        global stop_requested
        global successor
        global predecessor
        global other_neighbors

        stop_requested = False #####
        while not stop_requested:
            conn = http.client.HTTPConnection(successor) # 후임을 연결해서
            conn.request("GET", "/successor") # 후임을 요청하기
            resp = conn.getresponse()
            if resp.status != 200: # 후임이 정상 응답하지 않으면
                logging.info("Unresponded!! successor: {}, status: {}".format(successor, resp.status)) ##
                conn = http.client.HTTPConnection(other_neighbors) # 후임의 후임을 연결
                conn.request("GET", "/notify_predecessor/"+node_name)
                logging.debug("HTTPConnection to {}: /notify_predecessor/{}".format(other_neighbors, node_name)) ##
                conn.close() ## 닫기

                successor = other_neighbors # 나의 새로운 후임으로 후임의 후임을 할당
                conn = http.client.HTTPConnection(successor) # 새 후임을 연결
                conn.request("GET", "/successor") # 후임을 요청하기
                resp = conn.getresponse()
                if resp.status != 200:
                    successor_successor = None
                else:
                    body = resp.read()
                    successor_successor = body.decode("utf-8") # 후임의 답변(후임의 후임)을 후임의 후임에 저장
                conn.close()
                other_neighbors = successor_successor
                logging.info("successor: {}, successor's successor: {}".format(successor, other_neighbors)) ##
                
            else: # 후임이 정상 응답하면
                logging.info("Responded!! successor: {}, successor's successor: {}".format(successor, other_neighbors)) ##
                body = resp.read()
                successor_successor = body.decode("utf-8") # 후임의 답변(후임의 후임)을 후임의 후임에 저장
                other_neighbors = successor_successor
                logging.info("successor's successor: {}".format(other_neighbors)) ##

            time.sleep(0.1) # 0.1 second sleep try to change it

    ### 다른 스크립트에서, 네트워크가 안정화되었는지 테스트할 때는 (Network tolerance) 
    ### 후임을 따라 핑을 계속 돌려서 자기 노드 이름을 다시 만나면 원이 회복된 것으로 하고 스탑
    ### 못 만나면 원이 회복되지 않은 것으로 하고 스탑
    
    # Start server in a new thread, because server HTTPServer.serve_forever()
    # and HTTPServer.shutdown() must be called from separate threads
    thread = threading.Thread(target=server_main)
    thread.daemon = True
    thread.start()

    #Start stabilizer
    stabilization_thread = threading.Thread(target=stabilization) #####
    stabilization_thread.daemon = True #####
    stabilization_thread.start() #####

    # Shut down on kill (SIGTERM) and Ctrl-C (SIGINT)
    signal.signal(signal.SIGTERM, shutdown_server_on_signal)
    signal.signal(signal.SIGINT, shutdown_server_on_signal)

    #Start stabilizer
    # Stabilizer_thread = threading.Thread(target=Stabilizer, args=(args.port+1,)) #####
    # Stabilizer_thread.daemon = True #####
    # Stabilizer_thread.start() #####

    # Wait on server thread, until timeout has elapsed
    #
    # Note: The timeout parameter here is also important for catching OS
    # signals, so do not remove it.
    #
    # Having a timeout to check for keeps the waiting thread active enough to
    # check for signals too. Without it, the waiting thread will block so
    # completely that it won't respond to Ctrl-C or SIGTERM. You'll only be
    # able to kill it with kill -9.
    thread.join(args.die_after_seconds)
    stabilization_thread.join(args.die_after_seconds)
    #Stabilizer_thread.join(args.die_after_seconds) #####

    if thread.is_alive():
        logger.info("Reached %.3f second timeout. Asking server to shut down", args.die_after_seconds)
        server.shutdown()

    if stabilization_thread.is_alive(): #####
        stabilization_thread.join() #####
    #if Stabilizer_thread.is_alive(): #####
    #    Stabilizer_thread.join() #####
    

    logger.info("Exited cleanly")

if __name__ == "__main__":

    parser = arg_parser()
    args = parser.parse_args()
    run_server(args)
