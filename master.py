
import requests,math,re,sys,logging
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


logging.basicConfig(filename="master_log.log",filemode="w",format="Filename : %(filename)s--Line number: %(lineno)d--Process is: %(process)d--Time: %(asctime)s--%(message)s",level=logging.INFO)
if(sys.argv[1]):
        configuration_file=sys.argv[1]
        logging.info('Configuration file received from command line is %s',str(configuration_file))
else:
        logging.error('Missing configuration file..Pass configuration file as command line argument..Terminating program')
        print("Pass a configuration file while running this program")
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




def validate(configurations_list):
    if(configurations_list[0]==""):
        logging.error('Input data is missing..Terminating program')
        exit(0)
    if (configurations_list[1] not in ["word_count_map","inverted_index_map"]):
        logging.error('Map function  %s',str(configurations_list[1]),'is not supported')
        exit(0)
    if(configurations_list[2] not in ["word_count_reduce","inverted_index_reduce"]):
        logging.error('Reduce function  %s',str(configurations_list[2]),'is not supported')
        exit(0)
    if(configurations_list[3]==""):
        logging.error('Output location is missing..Terminating program')
        exit(0)
    return



def map_input(file,map_fn):
 try:
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    s=""
    for i in file:
        s+=i+":;"
    if(map_fn=="word_count_map"):
        command="set"+" "+str("word_count_map_input")+" "+str(len(s))+"\r\n"+str(s)+"\r\n"
    elif(map_fn=="inverted_index_map"):
        command="set"+" "+str("inverted_index_map_input")+" "+str(len(s))+"\r\n"+str(s)+"\r\n"
    logging.info('command being passed to server in "map_input" is %s',str(command))
    length=pack('>Q',len(command.encode()))
    soc.send(length)
    soc.send(command.encode())
    data=soc.recv(1400)
    result=data.decode()
    logging.info('Response received from server in "map_function" is %s',str(result))
    soc.close()
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return result



def shuffle(map_fn):
 try:
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    command=""
    if(map_fn=="word_count_map"):
        command="shuffle"+" "+str("word_count_map_result")+"\r\n"
    elif(map_fn=="inverted_index_map"):
        command="shuffle"+" "+str("inverted_index_map_result")+"\r\n"
    length=pack('>Q',len(command))
    soc.send(length)
    soc.send(command.encode())
    data=soc.recv(1400)
    result=data.decode()
    soc.close()
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return result

def store_output_file(reduce_fn,output_location):
 res=True
 try:
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    command=""
    if(reduce_fn=="word_count_reduce"):
        command="get"+" "+"word_count_reduce_result"+"\r\n"
    elif(reduce_fn=="inverted_index_reduce"):
        command="get"+" "+"inverted_index_reduce_result"+"\r\n"
    logging.info('Command sent to server in store_output_file is %s',str(command))
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
    if(reduce_fn=="word_count_reduce"):
        output="The output of word count map reduce task is "+"\n"
    elif(reduce_fn=="inverted_index_reduce"):
        output="The output of inverted index map reduce task is "+"\n"
        data=data.replace("::","  ")
        data=data.replace(":",",")
    result=data.splitlines()
    list=result[1].replace('(','')
    s=list.split(')')
    for i in s:
        output+=str(i)+"\n"
    file=open(output_location,"w+")
    file.write(output)
    file.close()
 except Exception as e:
     res=False
     logging.error("Exception occurred", exc_info=True)
 return res

def backup():
 try:
    res=False
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    command="backup"+"\r\n"
    length=pack('>Q',len(command.encode()))
    soc.send(length)
    soc.send(command.encode())
    data=soc.recv(1024)
    data=str(data.decode())
    soc.close()
    if(data.strip()=="DONE"):
        res=True
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return res






try:
    if(sys.argv[2]):
        configuration_file=sys.argv[2]
        logging.info('Configuration file received from command line is %s',str(configuration_file))
    else:
        logging.error('Missing configuration file..Pass configuration file as command line argument..Terminating program')
        exit(0)

    configurations_list=[]
    with open(configuration_file,"r") as config_file:
        logging.info('Reading the contents of the configuration file %s',configuration_file)
        configurations=json.load(config_file)
        input_data=configurations["input_data"]
        logging.debug('input data in the configuration file is %s',str(input_data))
        configurations_list.append(input_data)
        map_fn=configurations["map_fn"]
        logging.debug('map function in the configuration file is %s',str(map_fn))
        configurations_list.append(map_fn)
        reduce_fn=configurations["reduce_fn"]
        logging.debug('reduce function in the configuration file is %s',str(reduce_fn))
        configurations_list.append(reduce_fn)
        output_location=configurations["output_location"]
        logging.debug('output location in the configuration file is %s',str(output_location))
        configurations_list.append(output_location)
        validate(configurations_list)
        logging.info('calling validate to validate configuration file parameters')
    files=[]
    result=""
    if(map_fn=="word_count_map"):
        response = requests.get(input_data)
        soup=BeautifulSoup(response.text,'html.parser')
        logging.info('Parsing the input html file %s',input_data)
        s=""
        count=0
        for link in soup.find_all('p'):
            s+=link.text
        s_len=len(s)
        length=math.ceil(s_len//9)
        file_input=textwrap.wrap(s,length)
        logging.debug('Calling map_input to store input data in key-value store')
        result=map_input(file_input,map_fn)

    elif(map_fn=="inverted_index_map"):
        for i in input_data:
            response = requests.get(i)
            soup=BeautifulSoup(response.text,'html.parser')
            s=""
            for i in soup.find_all('p'):
                s+=i.text
            s=textwrap.fill(s)
            files.append(s)
        result=map_input(files,map_fn)

    map_result=""
    if(result.strip()=="STORED"):
        if(map_fn=="word_count_map"):
            logging.info('RPC call to word_count_mapper')
            proxy = xc.ServerProxy("http://%s:%s"%(word_count_mapper_ip,str(word_count_mapper_port)),allow_none=True)
            map_result=proxy.spool()
        elif(map_fn=="inverted_index_map"):
            logging.info('RPC call to inverted_index_mapper')
            proxy = xc.ServerProxy("http://%s:%s"%(inverted_index_mapper_ip,str(inverted_index_mapper_port)),allow_none=True)
            map_result=proxy.spool()
        if(map_result):
            logging.info('Map task successfully completed')
            logging.debug('Calling shuffle in master')
            shuffle_result=shuffle(map_fn)
            logging.info('The response of shuffle in master is %s',str(shuffle_result))
            shuffle_result=shuffle_result.strip()
            if(shuffle_result=="STORED"):
                logging.info('Shuffle task successfully completed')
                if(reduce_fn=="word_count_reduce"):
                    logging.info('RPC call to word_count reducer')
                    proxy = xc.ServerProxy("http://%s:%s"%(word_count_reducer_ip,str(word_count_reducer_port)),allow_none=True)
                    reducer_result=proxy.spool()
                elif(reduce_fn=="inverted_index_reduce"):
                    logging.info('RPC call to inverted_index_reducer')
                    proxy = xc.ServerProxy("http://%s:%s"%(inverted_index_reducer_ip,str(inverted_index_reducer_port)),allow_none=True)
                    reducer_result=proxy.spool()
                if(reducer_result):
                    logging.info('Reduce task successfully completed')
                    res=store_output_file(reduce_fn,output_location)
                    if(res):
                        logging.info('The output of the map reduce task has been stored in the file %s',output_location)
                        res=backup()
                        if(res):
                            logging.info('Map reduce task successfully completed')
                            print("Map reduce task successfully completed")
                        else:
                            logging.error("Backup failed")
                else:
                    logging.error('Reducer task %s',reduce_fn,'failed')
        else:
            logging.error('Mapper task %s',map_fn,'failed')

    else:
        logging.error('Storing input data in server failed')

except Exception as e:
  logging.error("Exception occurred", exc_info=True)
