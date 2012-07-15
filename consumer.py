from jafka_utils import JafkaUtils,ByteBuffer;
from messages import *;
import jafka_utils,socket;

class ByteBufferMessageSet(object):
	"""This is a message set"""
	def __init__(self,byteBuffer):
		self.byteBuffer = byteBuffer;
    	

class SimpleConsumer:

	def __init__(self,host,port,soTimeout = 30 * 1000,bufferSize = 64 * 1024):
		self.host = host;
                self.port = port;
		self.soTimeout = soTimeout;
		self.bufferSize = bufferSize;

        def connect(self):
                self.jafkaSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
                self.jafkaSocket.connect((self.host, self.port));
                print "connect to jafka server success!"

        def sendRequest(self,request):
                requestBytes = self.getRequestBytes(request);
                self.jafkaSocket.sendall(bytearray(requestBytes));
                responseBytes = self.jafkaSocket.recv(64 * 1024);#It is the same as java client
                responseByteBuffer = ByteBuffer(responseBytes,None);
                msgByteSize = responseByteBuffer.getInt();
                resultCode = responseByteBuffer.getShort();
                print "receive message size:"+str(msgByteSize);
                print("resultCode:"+str(resultCode));
                msgLength = responseByteBuffer.getInt();
                msgBytes = responseByteBuffer.getBytes(msgLength);
                stringMsg = StringMessage(msgBytes);
                print(stringMsg.getMessage());

        def getRequestBytes(self,request):
                requestKeySize = jafka_utils.SHORT_SIZE;
                totalSize = requestKeySize + request.getSizeInBytes();
                byteBuffer = ByteBuffer.allocate(totalSize);
                byteBuffer.putInt(totalSize);
                byteBuffer.putShort(request.getRequestKey());
                byteBuffer.putBytes(request.toBytes());
                return byteBuffer.array();

        def close(self):
                self.jafkaSocket.close();
                print "Jafka client is close!";
'''
This is a Request that support get message by some args
'''
class FetchRequest:
        def __init__(self,topic,partition,offset,maxSize):
		self.topic = topic;
		self.partition = partition;
		self.offset = offset;
		self.maxSize = maxSize;
        '''
        Change the request to bytes which send to server
        '''
        def toBytes(self):
                byteBuffer = ByteBuffer.allocate(self.getSizeInBytes());
                topicLen = len(self.topic);
                byteBuffer.putShort(topicLen);
                byteBuffer.putBytes(bytearray(self.topic));
                byteBuffer.putInt(self.partition);
                byteBuffer.putLong(self.offset);
                byteBuffer.putInt(self.maxSize);
                return byteBuffer.array();

        def getSizeInBytes(self):
                topicLen = JafkaUtils.getShortStringSize(self.topic);
                partitionLen = jafka_utils.INT_SIZE;
                offsetLen = jafka_utils.LONG_SIZE;
                maxSizeLen = jafka_utils.INT_SIZE;
                return topicLen + partitionLen + offsetLen + maxSizeLen;

        def getRequestKey(self):
                return 1;

#print JafkaUtils.getShortStringSize("aaa")
# print fetchRequest.toBytes();
consumer = SimpleConsumer("localhost",9092);
consumer.connect();
fetchRequest = FetchRequest("demo",0,0,64 * 1024);
consumer.sendRequest(fetchRequest);
consumer.close();
