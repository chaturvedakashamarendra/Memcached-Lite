from xmlrpc.server import SimpleXMLRPCServer
import multiprocessing
from multiprocessing import Value
import os,time,logging
import socket
from struct import pack,unpack
import tqdm
import re
logging.basicConfig(filename="word_count_map_log.log",filemode='w',format="Filename : %(filename)s--Line number: %(lineno)d--Process is: %(process)d--Time: %(asctime)s--%(message)s",level=logging.INFO)
import sys,json,logging
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





lock=multiprocessing.Lock()
server = SimpleXMLRPCServer((word_count_mapper_ip,word_count_mapper_port), logRequests=True,allow_none=True   )

#par=tqdm(total=l)
def map(n):
 try:
    lock.acquire()
    exp=False
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    st=""
    logging.info("Mapper process id for {0}".format(os.getpid()))
    s=re.findall(r'\w+',n)
    for i in s:
        st=st+"("+str(i)+":"+"1"+")"+","
    command="set"+" "+str("word_count_map_result")+" "+str(len(st))+"\r\n"+str(st)+"\r\n"
    length=pack('>Q',len(command.encode()))
    soc.send(length)
    logging.debug("Command sent to server from map is %s"%str(command))
    soc.send(command.encode())
    data=soc.recv(1400)
    d=data.decode()
    d=d.strip()
    soc.close()
    lock.release()
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return



def get_data():
 try:
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    command="get"+" "+"word_count_map_input"+"\r\n"
    logging.info('Command sent to server is %s',str(command))
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
 try:
    res=False
    data=get_data()
    l=data.splitlines()
    map_input=l[1].split(':;')
    map_input.pop()
    logging.info('Data received from the server')
    p = multiprocessing.Pool(10)
    logging.info('Starting the word count mappers')
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
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return res



server.register_function(spool)
server.serve_forever()

