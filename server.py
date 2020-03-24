import socket
import os
import threading
import itertools,logging
from struct import unpack,pack
import math
import re

logging.basicConfig(filename="server_log.log",filemode='w',format="Filename : %(filename)s--Line number: %(lineno)d--Process is: %(process)d--Time:%(asctime)s--%(message)s",level=logging.INFO)
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



msg=""
#Implementing multithreading
class multi_request(threading.Thread):
    def __init__(self,conn,address):
        threading.Thread.__init__(self)
        self.address=address
        self.conn=conn

    def run(self):
     global to_Read
     try:
        while True:
            msg=""
            bs=self.conn.recv(8)
            (length,)=unpack('>Q',bs)
            data=b''
            while len(data)< length:
                to_Read=length-len(data)
                data+=self.conn.recv(1024 if to_Read > 1024 else to_Read)
            st=str(data.decode())
            logging.debug("Data received by server is %s"%str(st))
            l=st.splitlines()
            l1=l[0].split()
            l2=l[1:]
            command=l1[0]
            if(command=="set"):
                logging.debug("Flow is inside set condition")
                exp=set(l1,l2)
                if(exp==0):
                    logging.info("Data is stored")
                    st="STORED\r\n"
                    self.conn.send(st.encode())
                else:
                    logging.info("Data is not stored")
                    self.conn.send("NOT-STORED\r\n".encode())
            elif(command=="get"):
                logging.debug("Flow is inside get condition")
                value,exp,msg=get_value(l1[1])
                logging.debug("Value retuened from get is %s"%str(value))
                #if the value is found
                if(exp==0 and value):
                    logging.info("Value being sent from server")
                    length=pack('>Q',len(value.encode()))
                    self.conn.send(length)
                    self.conn.send(value.encode())
                else:
                #else returns value not found
                    logging.debug("Value not found")
                    self.conn.send(msg.encode())
            elif(command=="shuffle"):
                logging.debug("Flow is inside shuffle condition")
                if(l1[1].strip()=="word_count_map_result"):
                    logging.info("Shuffle for word count called")
                    msg=shuffle(l1[1])
                elif(l1[1].strip()=="inverted_index_map_result"):
                    logging.info("Shuffle for inverted index called")
                    msg=shuffle_inverted_index(l1[1])
                if(msg=="STORED"):
                    logging.info("Shuffle is successfull")
                    st="STORED\r\n"
                    self.conn.send(st.encode())
                else:
                    logging.info("Shuffle is unsuccessfull")
                    self.conn.send("NOT-STORED\r\n".encode())
            elif(command=="backup"):
                logging.debug("Flow is inside backup")
                res=backup()
                if(res):
                    logging.info("Backup successful")
                    st="DONE\r\n"
                    self.conn.send(st.encode())
                else:
                    logging.error("Backup failed")
                    self.conn.send("Failed\r\n".encode())
     except Exception:
        pass

def shuffle_inverted_index(key):
 try:
    value,msg=check_key(key)
    value=value.strip()
    if(not value):
            logging.debug("Key does not exist")
            return "key does not exist"
    elif(msg):
            return msg
    s=value.replace('(','')
    st=s.split(')')
    st=[x for x in st if x!='']
    d={}
    for i in st:
        l=i.split(':')
        if l[0] in d:
            if l[1] is not d.get(l[0]):
                d[l[0]]=d.get(l[0])+":"+l[1]
        else:
            d[l[0]]=l[1]
    words_len=len(d)
    size1=math.ceil(words_len//2)
    i = iter(d.items())
    d1 = dict(itertools.islice(i,size1))
    d2 = dict(i)
    st=""
    for i in d1:
        st+="("+i+"::"+str(d1.get(i))+")"
    st+="*%*"
    for i in d2:
        st+="("+i+"::"+str(d2.get(i))+")"
    logging.info("Storing shuffle result")
    lock.acquire()
    st1="inverted_index_shuffle_result"+"^-^"+st
    file=open("store.txt","a+")
    file.write(st1+"\n")
    file.close()
    lock.release()
 except Exception as e:
        logging.error("Exception occurred", exc_info=True)
        return e
 return "STORED"



def shuffle(key):
    try:
            value,msg=check_key(key)
            value=value.strip()
            if(not value):
                logging.info("key does not exist")
                return "key does not exist"
            elif(msg):
                return msg
            l2=value.split(',')
            l2=[x for x in l2 if x!='']
            l2.sort()
            s,l,l3=[],[],[]
            for i in l2:
                if(i not in l3):
                    l3.append(i)
            for i in l3:
                for j in l2:
                    if(i==j):
                        s.append(i)
                l.append(s)
                s=[]
            n=len(l)
            l1=[]
            m=math.ceil(n/10)
            iter=9
            if(n<10):
                iter=n-1
            i=0
            while(i<iter):
                if(m>1):
                    l1.append(l[0:m])
                    del l[0:m]
                    i=i+1
                else:
                    l1.append(l[m])
                    del l[m]
                    i=i+1
            if l:
                l1.append(l)
            l1=[x for x in l1 if x!=[]]
            s=""

            f=[]
            sf=""
            for i in l1:
                for j in i:
                    s+="".join(j)
                sf+=s+","
                s=""
            logging.info("Storing shuffle result")
            st1="word_count_shuffle_result"+"^-^"+sf
            lock.acquire()
            file=open("store.txt","a+")
            file.write(st1+"\n")
            file.close()
            lock.release()
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
        return e
    return "STORED"

def backup():
 res=False
 try:
     if(os.path.exists("store.txt")):
         lock.acquire()
         os.rename("store.txt","store.txt_bkp")
         lock.release()
         res=True
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return res


def check_key(key):
  msg=""
  try:
    if(not os.path.exists("store.txt")):
        logging.info("File does not existing..Creating file store.txt to store the key-value")
        file=open("store.txt","r+")
        st=file.read()
        file.close()
    else:
        lock.acquire()
        file=open("store.txt","r+")
        st=file.read()
        file.close()
        lock.release()
    dict={}
    l=st.splitlines()
    #storing the key - value to a dictionary
    for i in l:
        st1=i.split('^-^')
        dict[st1[0]]=st1[1]
    #retrieving the value for the passed key
    value=dict.get(key)
    logging.debug("value in check key is",value)
  except FileNotFoundError:
    msg="File does not exist"
    return False,msg
  return value,msg

def set(l1,l2):
    exp=0
    key=l1[1]
    value=(''.join(l2)).strip()
    val,msg=check_key(key)
    if((not(val))):
        logging.debug("Key does not exist..Storing the new key-value")
        st1=key+"^-^"+value
        lock.acquire()
        file=open("store.txt","a+")
        file.write(st1+"\n")
        file.close()
        lock.release()
    #if file exists or if updated value of key is passed or multiple values of same key-value is passed
    else:
        lock.acquire()
        file=open("store.txt","r+")
        st=file.read()
        file.close()
        lock.release()
        dict={}
        lock.acquire()
        l=st.splitlines()
        #storing the key - value to a dictionary
        for i in l:
            st1=i.split('^-^')
            dict[st1[0]]=st1[1]
        dict[key]=val+value
        s=""
        for i in dict:
            s+=i+"^-^"+dict.get(i)+"\n"
        lock.release()
        logging.debug("Value being stored is %s"%s)
        logging.info("Value being stored by set")
        lock.acquire()
        file=open("store.txt","w")
        file.write(s)
        file.close()
        lock.release()
    return exp

def get_value(key):
 exp=0
 st1=""
 #getting the value for the key passed
 value,msg=check_key(key)
 if(value):
        st1="VALUE"+" "+key+" "+str(len(value))+"\r\n"+str(value)+"\n\r"+"END\n\r"
 else:
        #if the value for the key passed is not present
        if(not msg):
            logging.debug("Key not present")
            msg="key not present"
        exp=1
 logging.debug("Value sent from get_value is %s"%st1)
 return st1,exp,msg



try:
    host=server_ip
    port=server_port
    s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    s.bind((host,port))
    logging.info("server is listening")
    lock=threading.Lock()

    while True:
        s.listen(1)
        conn,address=s.accept()
        t=multi_request(conn,address)
        logging.info("Connection to client:%s"%str(address))
        t.start()
except Exception as e:
    logging.error("Exception occurred", exc_info=True)




