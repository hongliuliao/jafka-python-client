'''
Created on 2012-6-5

@author: hongliuliao
'''
import socket,struct;
from jafka_utils import ByteBuffer;

class ZooKeeper:
    def __init__(self,connectString,sessionTimeout):
        self.connectString = connectString;
        self.sessionTimeout = sessionTimeout;
        self.zkSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
        connectStrs = connectString.split(":");
        serverIp = connectStrs[0];
        serverPort = int(connectStrs[1]);
        self.zkSocket.connect((serverIp, serverPort));
        print "Telnet to zookeeper server success which serverIp:"+serverIp+",serverPort:"+str(serverPort);
        connRequestList = [0, 0, 0, 44, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 117, 48, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        connRequest = bytearray(connRequestList);
        self.zkSocket.sendall(str(connRequest));
        self.zkSocket.recv(1024);#get connect response
    def getData(self,path):
        requestBytes = [0, 0, 0, -1, 0, 0, 0, 1, 0, 0, 0, 4];
        pathLen = len(path);
        pathLenList = list(struct.unpack("4b",struct.pack("!i",pathLen)));
        for pathLenByte in pathLenList:
            requestBytes.append(pathLenByte);#add path lenth
        pathByteList =  list(struct.unpack(str(pathLen)+"b",struct.pack("!" + str(pathLen) + "s",path)));
        for pathByte in pathByteList:
            requestBytes.append(pathByte);#add path
        requestBytes.append(0);#add ifwatch
        bodySize = len(requestBytes) - 4;
        bodySizeList = list(struct.unpack("4b",struct.pack("!i",bodySize)));
        for i in range(4):
            requestBytes[i] = bodySizeList[i];
        #print requestBytes;
        trueBytes = bytearray(requestBytes);
        self.zkSocket.sendall(trueBytes);
        responseBytes = self.zkSocket.recv(1024);
        byteBuffer = ByteBuffer(responseBytes);
        byteBuffer.getInt();#msglen
        byteBuffer.getInt();#xid
        byteBuffer.getLong();#zxid
        byteBuffer.getInt();#err
        resultValue = byteBuffer.getString();
        return resultValue;

    def close(self):
        self.zkSocket.close();

# zk = ZooKeeper("zk1.in.i.sohu.com:2181",30000);
# zkData = zk.getData("/talent/suc/cache/friend");
# zk.close();
# print zkData;
