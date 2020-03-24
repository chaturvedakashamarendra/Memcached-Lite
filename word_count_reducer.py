from xmlrpc.server import SimpleXMLRPCServer
import multiprocessing
import os,time
import re
import socket
from struct import pack,unpack
lock=multiprocessing.Lock()
import sys,json,logging
logging.basicConfig(filename="word_count_reducer_log.log",filemode="w",format="Filename : %(filename)s--Line number: %(lineno)d--Process is: %(process)d--Time:%(asctime)s--%(message)s",level=logging.INFO)
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




server = SimpleXMLRPCServer((word_count_reducer_ip, word_count_reducer_port), logRequests=True,allow_none=True   )

def reduce(list):
 try:
    lock.acquire()
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    logging.info("Reducer process id for {0}".format(os.getpid()))
    list=list.replace('(','')
    s=list.split(')')
    s=[x for x in s if x!='']
    words,unique_words=[],[]
    for i in s:
        st=i.split(':')
        words.append(st[0])
    words=[x for x in words if x!=[]]
    for i in words:
        if(i not in unique_words):
            unique_words.append(i)
    st=""
    for i in unique_words:
        st+="("+str(i)+":"+str(words.count(i))+")"
    command="set"+" "+str("word_count_reduce_result")+" "+str(len(st))+"\r\n"+str(st)+"\r\n"
    logging.debug("Command sent to server from reduce is %s"%str(command))
    length=pack('>Q',len(command.encode()))
    soc.send(length)
    soc.send(command.encode())
    data=soc.recv(1400)
    logging.debug("Data received from server in reduce")
    soc.close()
    lock.release()
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return


def spool():
 try:
    res=False
    p = multiprocessing.Pool(5)
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    command="get"+" "+"word_count_shuffle_result"+"\r\n"
    logging.debug("Command sent to server from spool is %s"%str(command))
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
    d=data.splitlines()
    shuffled_input=[]
    for i in d:
        final=i.split(',')
        shuffled_input.append(final)

    shuffled_input[1]=[x for x in shuffled_input[1] if x!='']
    logging.debug("Input to reducer is %s"%str(shuffled_input[1]))
    logging.info("Starting the reducer processes")
    rs=p.map_async(reduce,shuffled_input[1])
    p.close()
    while (True):
        if (rs.ready()):
            res=True
            break
        remaining = rs._number_left
        logging.debug("Waiting for", remaining, "tasks to complete...")
        time.sleep(0.5)
    logging.info("Sending response to master")
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return res




server.register_function(spool)
server.serve_forever()
