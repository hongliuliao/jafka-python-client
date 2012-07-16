from jafka_utils import JafkaUtils,ByteBuffer;
from messages import *;
import jafka_utils,socket;


class ByteBufferMessageSet(object):
	"""This is a message set"""
	def __init__(self,allMsgBytes):
		self.allMsgBytes = allMsgBytes;
    	

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
                # print("receive responseBytes:"+str(len(responseBytes)));
                responseByteBuffer = ByteBuffer(responseBytes,None);
                msgByteSize = responseByteBuffer.getInt();
                resultCode = responseByteBuffer.getShort();
                while(len(responseBytes) != msgByteSize + 4):#if not receive completely we will get when read complete
                    responseBytes = responseBytes + self.jafkaSocket.recv(64 * 1024);
                responseByteBuffer = ByteBuffer(responseBytes,None);
                responseByteBuffer.currentIndex = responseByteBuffer.currentIndex + 6;#add 6 is because the responseBytes is new and we don't need read msgByteSize and resultCode
                # print("receive responseBytes length:"+str(len(responseBytes)));
                # print "receive message size:"+str(msgByteSize);
                # print("resultCode:"+str(resultCode));
                msgBodySize = responseByteBuffer.getInt();
                msgSizes = responseByteBuffer.getBytes(msgBodySize);
                stringMsg = StringMessage(msgSizes);
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
fetchRequest = FetchRequest("demo",0,488,64 * 1024);
consumer.sendRequest(fetchRequest);
consumer.close();
