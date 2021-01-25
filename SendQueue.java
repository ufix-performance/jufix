package jufix;

import java.util.concurrent.locks.ReentrantLock;

class SendQueue {
    
    public static final int QUEUE_OPEN = 0;
    public static final int QUEUE_CLOSE = 1;

    private int qIndex;

    private ReentrantLock copyLock;

    private Flag qFlag;
    private int qFlagThreshold;

    private FileHandler dataFile;

    private byte[] qBuf;
    private int qBufPos;
    private int qBufSize;

    private SendQueueInfo[] msgsQueue;
    private int qSize;
    private int qMaxMsgs;

    private volatile long dataSeq;
    private long recoverDataSeq;

    private long headDataSeq;
    
    public SendQueue(int index, int size, int reserve, int avgMsgSize, int maxMsgSize, 
		     FileHandler file, FileHandler recoverFile) {
	qIndex = index;
	qSize = size;
	qMaxMsgs = size - reserve;

	dataFile = file;

	copyLock = new ReentrantLock();
    
	qFlag = new Flag(QUEUE_CLOSE);
	qFlagThreshold = QUEUE_OPEN;

	qBufSize = qSize*avgMsgSize;
	qBuf = new byte[qBufSize + maxMsgSize];
	qBufPos = 0;
	
	msgsQueue = new SendQueueInfo[qSize];
	for (int i = 0; i < qSize; i++) {
	    msgsQueue[i] = new SendQueueInfo();
	}
	dataSeq = 1;
	recoverDataSeq = 1;
	headDataSeq = 1;
	
	recoverData(recoverFile);
    }
    
    private void recoverData(FileHandler file) {
	if (file == null) return;
	int bufSize = (1 << 24);
	byte[] buf = new byte[bufSize];

	int bufStart = 0;

	while (true) {
	    int nbytes = file.read(buf, bufStart, bufSize - bufStart);
	    if (nbytes <= 0) break;  
	    bufStart = recoverMsgs(buf, bufStart + nbytes);
	}
	dataSeq = recoverDataSeq;
	System.out.println("Recover data seq " + recoverDataSeq);
    }

    private int recoverMsgs(byte[] buf, int bufLen) {
	int msgStart = 0;
	
	while (true) {
	    if (msgStart >= bufLen) {
		return 0;
	    }

	    while (true) {
		if (msgStart + Constants.INT_BYTES > bufLen) {
		    System.arraycopy(buf, msgStart, buf, 0, bufLen - msgStart);
		    return bufLen - msgStart;
		}
		if (Util.convertCTOI(buf[msgStart + Constants.INT_BYTES]) == Constants.BEGIN_STRING && 
		    buf[msgStart + Constants.INT_BYTES + 1] == Constants.EQUAL &&
		    buf[msgStart + Constants.INT_BYTES + 2] == 'F' &&
		    buf[msgStart + Constants.INT_BYTES + 3] == 'I') {
		    break;
		} else {
		    msgStart += 4;
		}
	    }
      
	    int msgLen = Util.convertToInt(buf, msgStart);
	    
	    if (msgStart + Constants.INT_BYTES + msgLen > bufLen) {
		System.arraycopy(buf, msgStart, buf, 0, bufLen - msgStart);
		return bufLen - msgStart;
	    }
	    recoverMsg(buf, msgStart + Constants.INT_BYTES, msgLen);
	    msgStart += (Constants.INT_BYTES + msgLen);
	}
    }
  
    private void recoverMsg(byte[] msg, int msgStartPos, int msgLen) {
	System.arraycopy(msg, msgStartPos, qBuf, qBufPos, msgLen); 
	int index = Util.getQueueIndex(recoverDataSeq, qSize);
	
	SendQueueInfo info = msgsQueue[index];
	info.dataSeq = recoverDataSeq;
	info.data = qBuf;
	info.dataStartPos = qBufPos;
	info.dataLength = msgLen;
	qBufPos += msgLen;
	if (qBufPos > qBufSize) {
	    qBufPos = 0;
	}
	recoverDataSeq++;
    }
    
    private void writeData(byte[] data, int dataStartPos, int len) {
	byte[] buf = new byte[Constants.INT_BYTES];
	Util.convertToBytes(len, buf, 0);
	dataFile.write(buf, 0, Constants.INT_BYTES);
	dataFile.write(data, dataStartPos, len);
	dataFile.flush();
    }

    public void setQFlagRef(Flag flag) {
	qFlag = flag;
    }
    
    public void setQFlagThreshold(int threshold) {
	qFlagThreshold = threshold;
    }

    public Flag getQFlagRef() {
	return qFlag;
    }

    public boolean isClose() {
	boolean isClose = (qFlag.getVal() < qFlagThreshold);
	if (isClose && qFlag.getVal() > QUEUE_CLOSE) {
	    qFlag.increase();
	}
	return isClose; 
    }
    
    public void setOpen() {
	qFlag.setVal(QUEUE_OPEN);
    }
  
    public void setClose() {
	qFlag.setVal(QUEUE_CLOSE);
    }

    public void markMsgSent(WMessage msg) {
	headDataSeq++;
    }

    public void markSeqSent(long seq) {
	if (seq < headDataSeq) return;
	if (seq >= recoverDataSeq) return;
	headDataSeq = seq + 1;
    }

    private boolean isQueueFull() {
	return ((int) (dataSeq - headDataSeq) >= qMaxMsgs);
    }

    private boolean isQueueEmpty() {
	return (dataSeq == headDataSeq);
    }
    
    public int getNumPendingMsgs() {
	return (int) (dataSeq - headDataSeq);
    }
    
    public void lock() {
	copyLock.lock();
    }

    public void unlock() {
	copyLock.unlock();
    }
    
    public int addMsg(WMessage msg, int numRetries) {
	msg.preFinalize();
	byte[] data = msg.getData();
	int dataStartPos = msg.getDataStartPos();
	int dataLength = msg.getDataLength();
	lock();
	while (isQueueFull()) {
	    unlock();
	    numRetries--;
	    if (numRetries == 0) return 0;
	    Util.sleep(1000);
	    lock();
	}

	writeData(data, dataStartPos, dataLength);
	if (!msg.isDirectBuf()) {
	    System.arraycopy(data, dataStartPos, qBuf, qBufPos, dataLength);
	}
	int index = Util.getQueueIndex(dataSeq, qSize);
	
	SendQueueInfo info = msgsQueue[index];
	
	info.data = qBuf;
	info.dataStartPos = qBufPos;
	info.dataLength = dataLength;
	info.dataSeq = dataSeq;
	qBufPos += dataLength;
	if (qBufPos > qBufSize) {
	    qBufPos = 0;
	}
	dataSeq++;
	unlock();
	return 1;
    }
  
    public int getMsg(WMessage msg) {
	if (isQueueEmpty()) return 0;
    
	int head = Util.getQueueIndex(headDataSeq, qSize);
	SendQueueInfo info = msgsQueue[head];
	msg.switchData(info.data, info.dataStartPos, info.dataLength);
	msg.setDataSeq(info.dataSeq);
	msg.indexSeq = headDataSeq;
	return 1;
    }

    public boolean hasMsg() {
	return !(isQueueEmpty());
    }

    public int getSentMsg(WMessage msg, long seq) {
	int index = Util.getQueueIndex(seq, qSize);
	SendQueueInfo info = msgsQueue[index];
	if (info.dataSeq != seq) {
	    if (info.dataSeq < seq) {
		int headIndex = Util.getQueueIndex(headDataSeq, qSize);
		if (msgsQueue[headIndex].dataSeq != 0 && msgsQueue[headIndex].dataSeq + qSize >= seq) {
		    markSeqSent(seq - 1);
		}
	    } else {
		markSeqSent(seq);
	    }
	    return 0;
	} else {
	    msg.switchData(info.data, info.dataStartPos, info.dataLength);
	    msg.setDataSeq(info.dataSeq);
	    markSeqSent(seq);
	    return 1;
	}
    }
    
    public byte[] getNewMsgBuf() {
	return qBuf;
    }

    public int getNewMsgPos() {
	return qBufPos;
    }

    public long getHeadDataSeq() {
	return headDataSeq;
    }
}

 
