from jafka_utils import ByteBuffer;

'''
A abstract base message
'''
class Message:
	def __init__(self,msgBytes):
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

# testBytes = [1,0,1,1,1,1,97,98,99,100,101];
# stringMsg = StringMessage(bytearray(testBytes));
# print(stringMsg.getMessage());

