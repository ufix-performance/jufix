package jufix;

import java.util.Arrays;
  
public class WMessage {

    private static final int NEW = 0;
    private static final int PRE_FIN = 1;

    private int qIndex;
    private int maxBodyLengthSize;

    private int status;
    private int directBufFlag;
    
    private byte[] data;
    private int dataStartPos;
    private int dataLength;

    private long seq;
    private long dataSeq;

    private int bodyLengthPos;
    private int bodyEndPos;

    public long indexSeq;

    public WMessage(int index) {
	status = PRE_FIN;
	qIndex = index;
    }

    public WMessage(int index, byte[] message, int messageStartPos) {
	status = NEW;
	qIndex = index;
	data = message;
	dataStartPos = messageStartPos;
	dataLength = 0;
	if (message == null) {
	    directBufFlag = 1;
	} else {
	    directBufFlag = 0;
	}
    }
  
    private void setChecksum(byte[] msg, int checksumStartPos, int cks) {
	cks %= 256;
	int digit = cks/100;
	msg[checksumStartPos] = Util.convertITOC(digit);
	cks -= digit*100;
	digit = cks/10;
	msg[checksumStartPos + 1] = Util.convertITOC(digit);
	cks -= digit*10;
	msg[checksumStartPos + 2] = Util.convertITOC(cks);
    }
    
    private void setTag(int tag) {
	int nbytes = Util.convertITOA(tag, data, dataStartPos + dataLength);
        dataLength += nbytes;
        data[dataStartPos + dataLength++] = Constants.EQUAL;
    }

    public int getQIndex() {
	return qIndex;
    }
  
    public void setQIndex(int index) {
	if (qIndex < 0) qIndex = index;
    }

    public void setMaxBodyLengthSize(int size) {
	maxBodyLengthSize = size;
    }

    public byte[] getData() {
	return data;
    }

    public int getDataStartPos() {
	return dataStartPos;
    }
    
    public int getDataLength() {
	return dataLength;
    }
    
    public String getMsg() {
	return new String(data, dataStartPos, dataLength);
    }

    public void switchData(byte[] msg, int msgStartPos, int msgLength) {
	data = msg;
	dataStartPos = msgStartPos;
	dataLength = msgLength;
    }
  
    public boolean isDirectBuf() {
	return (directBufFlag == 1);
    }

    public void reserveField(int tag, int len) {
	setTag(tag);
	Arrays.fill(data, dataStartPos + dataLength, dataStartPos + dataLength + len, (byte) '0');
	dataLength += len;
	data[dataStartPos + dataLength++] = Constants.SOH;
    }

    public long getSeq() {
	return seq;
    }

    public long getDataSeq() {
	return dataSeq;
    }

    public void setDataSeq(long seqNum) {
	dataSeq = seqNum;
    }

    
    public void setField(int tag, byte[] value, int offset, int len) {
	setTag(tag);

	System.arraycopy(value, offset, data, dataStartPos + dataLength, len);
	dataLength += len;
	data[dataStartPos + dataLength++] = Constants.SOH;
    }
  
    public void setField(int tag, byte[] value, int offset) {
	setTag(tag);
    
	int pos = offset;	
		
	while (value[pos] != (byte) '\0' && value[pos] != Constants.SOH) {
	    data[dataStartPos + dataLength++] = value[pos];
	    pos++;
	}
	data[dataStartPos + dataLength++] = Constants.SOH;
    }
    
    public void setField(int tag, String value) {
	setTag(tag);

	for (int i = 0; i < value.length(); i++) {
	    data[dataStartPos + dataLength++] = (byte) value.charAt(i);
	}
	data[dataStartPos + dataLength++] = Constants.SOH;
    }

    public void setField(int tag, int value) {
	setTag(tag);
	int nbytes = Util.convertITOA(value, data, dataStartPos + dataLength);
	dataLength += nbytes;
	data[dataStartPos + dataLength++] = Constants.SOH;
    }

    public void setField(int tag, double value) {
	setField(tag, String.valueOf(value));
    }

    public void setField(int tag, long value) {
	setTag(tag);
	int nbytes = Util.convertLTOA(value, data, dataStartPos + dataLength);
	dataLength += nbytes;
	data[dataStartPos + dataLength++] = Constants.SOH;
    }
  
    public void setField(int tag, char value) {
	setTag(tag);
	data[dataStartPos + dataLength++] = (byte) value;
	data[dataStartPos + dataLength++] = Constants.SOH;
    }

    public void copy(int tag, RMessage rmsg) {
	FieldInfo info = rmsg.getField(tag);
	if (info != null) {
	    setField(tag, info.data, info.valStartPos, info.valLength);
	}
    }
    
    public void markBodyLengthPos(int pos) {
	bodyLengthPos = pos;
    }
  
    public void markBodyEndPos(int pos) {
	bodyEndPos = pos;
    }
  
    public void preFinalize() {
	Util.convertITOA(dataLength - bodyLengthPos - maxBodyLengthSize - 1, 
			 data, dataStartPos + bodyLengthPos, dataStartPos + bodyEndPos);
	setTag(Constants.CHECKSUM);
	
	int cks = 0;
	int endPos = dataStartPos + dataLength - Constants.CHECKSUM_TAG_PART_SIZE;
	for(int i = dataStartPos; i < endPos; i++) {
	    cks += (int) data[i];
	}
	
	setChecksum(data, dataStartPos + dataLength, cks);
		
	dataLength += Constants.CHECKSUM_TAG_PART_SIZE;
	data[dataStartPos + dataLength++] = Constants.SOH;
	status = PRE_FIN;
    }
    
    public void finalize(int msgSeqPos, byte[] msgSeq, int msgSeqLength,
			 int sendingTimePos, byte[] utcTime, boolean resendFlag) {
	if (!resendFlag) {
	    finalizeNew(msgSeqPos, msgSeq, msgSeqLength,
			sendingTimePos, utcTime);
	} else {
	    finalizeResend(msgSeqPos, msgSeq, msgSeqLength,   
			   sendingTimePos, utcTime); 
	}
    }

    private void finalizeNew(int msgSeqPos, byte[] msgSeq, int msgSeqLength, 
			     int sendingTimePos, byte[] utcTime) {
	if (status == NEW) {
	    preFinalize();
	}
	int cks = Util.convertATOI(data, dataStartPos + dataLength - Constants.CHECKSUM_VALUE_PART_SIZE);
	
	System.arraycopy(msgSeq, 0, data, dataStartPos + msgSeqPos, msgSeqLength);
	seq = Util.convertATOL(msgSeq, 0, msgSeqLength);
    
	for (int i = 0; i < msgSeqLength; i++) {
	    cks += ((int) msgSeq[i] - (int) '0');
	}
	
	System.arraycopy(utcTime, 0, data, dataStartPos + sendingTimePos, Constants.SENDING_TIME_SIZE);
	System.arraycopy(utcTime, 0, data, dataStartPos + sendingTimePos + Constants.DISTANCE_SENDING_ORIG_SENDING, 
			 Constants.SENDING_TIME_SIZE);
	
	for (int i = 0; i < Constants.SENDING_TIME_SIZE; i++) {
	    cks += ((utcTime[i] - '0') << 1);
	}
	setChecksum(data, dataStartPos + dataLength - Constants.CHECKSUM_VALUE_PART_SIZE, cks);
    }
  
    private void finalizeResend(int msgSeqPos, byte[] msgSeq, int msgSeqLength, 
				int sendingTimePos, byte[] utcTime) {
	
	System.arraycopy(msgSeq, 0, data, dataStartPos + msgSeqPos, msgSeqLength); 
	seq = Util.convertATOL(msgSeq, 0, msgSeqLength);
	
	data[dataStartPos + sendingTimePos - 5] = 'Y';
	
	System.arraycopy(utcTime, 0, data, dataStartPos + sendingTimePos, Constants.SENDING_TIME_SIZE);
	if (data[dataStartPos + sendingTimePos + Constants.DISTANCE_SENDING_ORIG_SENDING] == (byte) '0') {
	    System.arraycopy(utcTime, 0, data, dataStartPos + sendingTimePos + Constants.DISTANCE_SENDING_ORIG_SENDING, 
			     Constants.SENDING_TIME_SIZE);
	}
    
	int cks = 0;
	for(int i = dataStartPos; i < dataStartPos + dataLength - Constants.CHECKSUM_SIZE; i++) {
	    cks += (int) data[i];
	}
	
	setChecksum(data, dataStartPos + dataLength - Constants.CHECKSUM_VALUE_PART_SIZE, cks);
    }
}
