import requests,math,re
import textwrap
from bs4 import BeautifulSoup
import socket
import time
import multiprocessing
import os
from struct import pack,unpack
import re
import json
import xmlrpc.client as xc
from xmlrpc.server import SimpleXMLRPCServer
lock=multiprocessing.Lock()
import sys,json,logging
logging.basicConfig(filename="inverted_index_mapper_log.log",filemode="w",format="Filename : %(filename)s--Line number: %(lineno)d--Process is: %(process)d--Time: %(asctime)s--%(message)s",level=logging.INFO)
if(sys.argv[1]):
        configuration_file=sys.argv[1]
        logging.info('Configuration file received from command line is %s',str(configuration_file))
else:
        logging.error('Missing configuration file..Pass configuration file as command line argument..Terminating program')
        exit(0)
configuration=[]
with open(configuration_file,"r") as config_file:
    configurations=json.load(config_file)
    server_ip=configurations["server_ip"]
    server_port=int(configurations["server_port"])
    word_count_mapper_ip=configurations["word_count_mapper_ip"]
    word_count_mapper_port=int(configurations["word_count_mapper_port"])
    word_count_reducer_ip=configurations["word_count_reducer_ip"]
    word_count_reducer_port=int(configurations["word_count_reducer_port"])
    inverted_index_mapper_ip=configurations["inverted_index_mapper_ip"]
    inverted_index_mapper_port=int(configurations["inverted_index_mapper_port"])
    inverted_index_reducer_ip=configurations["inverted_index_reducer_ip"]
    inverted_index_reducer_port=int(configurations["inverted_index_reducer_port"])




server = SimpleXMLRPCServer((inverted_index_mapper_ip, inverted_index_mapper_port), logRequests=True,allow_none=True   )


def map(s):
 try:
    lock.acquire()
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    logging.info("Mapper process id for {0}".format(os.getpid()))
    words=re.findall(r'\w+',s)
    unique_words=[]
    for i in words:
        if(i not in unique_words):
            unique_words.append(i)
    doc="doc_"+str(os.getpid())
    st=""
    for i in unique_words:
        st+="("+str(i)+":"+str(doc)+"|"+str(words.count(i))+")"
    command="set"+" "+str("inverted_index_map_result")+" "+str(len(st))+"\r\n"+str(st)+"\r\n"
    logging.debug("Command sent to server from map is %s"%str(command))
    length=pack('>Q',len(command.encode()))
    soc.send(length)
    soc.send(command.encode())
    data=soc.recv(1400)
    d=data.decode()
    soc.close()
    lock.release()
    logging.debug("Data received from server in map")
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return



def get_data():
 try:
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    command="get"+" "+"inverted_index_map_input"+"\r\n"
    logging.debug('Command sent to server from get_data is %s'%str(command))
    length=pack('>Q',len(command.encode()))
    soc.send(length)
    soc.send(command.encode())
    bs=soc.recv(8)
    (length,)=unpack('>Q',bs)
    data=b''
    while len(data)< length:
        to_Read=length-len(data)
        data+=soc.recv(1024 if to_Read > 1024 else to_Read)
    data=str(data.decode())
    soc.close()
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return data

def spool():
    res=False
    data=get_data()
    l=data.splitlines()
    map_input=l[1].split(':;')
    map_input.pop()
    logging.debug('The input to mapper is %s'%str(map_input))
    p = multiprocessing.Pool(10)
    logging.info('Starting mapper processes')
    rs=p.map_async(map,map_input)
    p.close()
    while (True):
        if (rs.ready()):
            res=True
            break
        remaining = rs._number_left
        logging.debug("Waiting for", remaining, "tasks to complete...")
        time.sleep(0.5)
    logging.info("Sending response to master")
    return res



server.register_function(spool)
server.serve_forever()




