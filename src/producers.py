from messages import StringMessage;
from jafka_utils import ByteBuffer;
import socket;

class Producer:

	def __init__(self,host,port):
		self.host = host;
		self.port = port;

	def connect(self):
		self.jafkaSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
                self.jafkaSocket.connect((self.host, self.port));

	def sendMessage(self,topicName,strMessages):
		sendBytes = bytearray(ProducerRequest(topicName,strMessages).toBytes());
		self.jafkaSocket.sendall(sendBytes);

class ProducerData:

	def __init__(self,datas):
		self.datas = datas;
		self.buildMessage();

	def buildMessage(self):
		self.messages = [];
		for data in self.datas:
			strMessage = StringMessage();
			strMessage.buildMessage(data)
			self.messages.append(strMessage);
		return self.messages;


class ProducerRequest:

	def __init__(self,topicName,strMessages):
		self.topicName = topicName;
		producerData = ProducerData(strMessages);
		self.messages = producerData.buildMessage();
		self.requestKey = 0;
		self.partition = 0;
		self.magic = 1;
		self.attribute = 0;
		self.crc32 = 0;

	def toBytes(self):
		self.totalBytes = [];
		self.totolMessageBytesSize = 0;

		for message in self.messages:
			self.totolMessageBytesSize = self.totolMessageBytesSize + len(message.toByteArray());

		messageBodyByteBuffer = ByteBuffer();
		messageBodyByteBuffer.putShort(0);#requestKey
		messageBodyByteBuffer.putShort(len(bytearray(self.topicName)));
		messageBodyByteBuffer.putBytes(bytearray(self.topicName));
		messageBodyByteBuffer.putInt(-1);#partition
		messageBodyByteBuffer.putInt(self.totolMessageBytesSize);

		for message in self.messages:
			messageBodyByteBuffer.putBytes(message.toByteArray());

		totolByteBuffer = ByteBuffer();
		totolByteBuffer.putInt(len(messageBodyByteBuffer.array()));
		totolByteBuffer.putBytes(messageBodyByteBuffer.array());
		return totolByteBuffer.array();


# request = ProducerRequest("demo",["a","b"]);
# print request.toBytes();

# producter = Producer("localhost",9092);
# producter.connect();
# producter.sendMessage("demo",["liao","haha"]);	