from jafka_utils import ByteBuffer;
import binascii;

'''
A abstract base message
'''
class Message:

	def __init__(self,msgBytes = None):
		if(msgBytes != None):
			self.msgBytes = msgBytes;
			self.bodySize = len(msgBytes);
			self.byteBuffer = ByteBuffer(msgBytes);
			self.version = self.byteBuffer.get();
			self.attribute = self.byteBuffer.get();
			self.crc32 = self.byteBuffer.getInt();
			self.msgSize = self.bodySize - 6; #6 bytes are version size + attribute size + crc32 bytes

class StringMessage(Message):

	def getMessage(self):
		return str(self.byteBuffer.getBytes(self.msgSize));

	def buildMessage(self,messageStr):
		self.byteBuffer = ByteBuffer();
		self.byteBuffer.put(1);#magic
		self.byteBuffer.put(0);#attribute
		crcValue = binascii.crc32(messageStr);
		self.byteBuffer.putInt(crcValue);#crc32
		self.byteBuffer.putBytes(bytearray(messageStr));
		self.messageSize = len(self.byteBuffer.array());


	def toByteArray(self):
		messageByteBuffer = ByteBuffer();
		messageByteBuffer.putInt(self.messageSize);
		messageByteBuffer.putBytes(self.byteBuffer.array());
		return messageByteBuffer.array();

# testBytes = [1,0,1,1,1,1,97,98,99,100,101];
# stringMsg = StringMessage(bytearray(testBytes));
# print(stringMsg.getMessage());
