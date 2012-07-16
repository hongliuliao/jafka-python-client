import socket,struct;

SHORT_SIZE = 2;
INT_SIZE = 4;
LONG_SIZE = 8;

class JafkaUtils(object):
    @staticmethod
    def getShortStringSize(strs):
        return SHORT_SIZE + len(strs);


class ByteBuffer:

        def __init__(self,inputBytes,capacity=None):
    		if(inputBytes == None):
    			self.inputBytes = [];
    			self.limit = capacity;
    			if(capacity == None):
    				self.capacity = len(self.inputBytes);
    	                else:
    				self.capacity = capacity
    		else:
    			self.inputBytes = inputBytes;
    			self.limit = len(inputBytes);
    		self.currentIndex = 0;

        @staticmethod
        def allocate(capacity):
                return ByteBuffer(None,capacity);

        def putShort(self,shortValue):
                shortBytes = struct.pack("!h",shortValue);
                for i in range(SHORT_SIZE):
                        self.inputBytes.append(shortBytes[i]);
                self.currentIndex = self.currentIndex + 2;

        def putInt(self,intValue):
                intBytes = struct.pack("!i",intValue);
                for i in range(INT_SIZE):
                        self.inputBytes.append(intBytes[i]);
                self.currentIndex = self.currentIndex + INT_SIZE;

        def putLong(self,longValue):
                longBytes = struct.pack("!q",longValue);
                for i in range(LONG_SIZE):
                        self.inputBytes.append(longBytes[i]);
                self.currentIndex = self.currentIndex + LONG_SIZE;

        def putBytes(self,bytes):
                for i in range(len(bytes)):
                        self.inputBytes.append(bytes[i]);
                self.currentIndex = self.currentIndex + len(bytes);

        def get(self):
                oneByte = self.inputBytes[self.currentIndex];
                self.currentIndex = self.currentIndex + 1;
                return oneByte;
        def getShort(self):
                shortBytes = [];
                for i in range(self.currentIndex,self.currentIndex + 2):
                    shortBytes.append(self.inputBytes[i]);
                self.currentIndex = self.currentIndex + 2;
                #change bytes to int
                shortValue = struct.unpack("!h",str(bytearray(shortBytes)))[0];
                return shortValue;
        def getInt(self):
                intBytes = [];
                for i in range(self.currentIndex,self.currentIndex + 4):
                    intBytes.append(self.inputBytes[i]);
                self.currentIndex = self.currentIndex + 4;
                #change bytes to int
                intValue = struct.unpack("!i",str(bytearray(intBytes)))[0];
                return intValue;
        def getLong(self):
                longBytes= []; 
                for i in range(self.currentIndex,self.currentIndex + 8):
                    longBytes.append(self.inputBytes[i]); 
                self.currentIndex = self.currentIndex + 8;
                longValue = struct.unpack(">q",str(bytearray(longBytes)))[0];
                return longValue;
        def getString(self):
                strBytes = [];
                strLen = self.getInt();
                for i in range(self.currentIndex,self.currentIndex + strLen):
                    strBytes.append(self.inputBytes[i]);
                self.currentIndex = self.currentIndex + strLen;
                #change bytes to string
                #strValue = struct.unpack(str(strLen) + "s",strBytes)[0];
                return str(bytearray(strBytes));
        def getBytes(self,byteSize):
                bytesArray = [];
                for i in range(self.currentIndex,self.currentIndex + byteSize):
                    bytesArray.append(self.inputBytes[i]);
                self.currentIndex = self.currentIndex + byteSize;
                return bytearray(bytesArray);

        def remaining(self):
                position = self.currentIndex;
                return self.limit - position;
        
        def array(self):
            return self.inputBytes;

# byteBuffer = ByteBuffer.allocate(1);
