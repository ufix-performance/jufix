package jufix;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Map;
import java.util.HashMap;

public abstract class Session extends Locker {
    
    private static final String DEFAULT_P_DIR = "data";    

    public static final int STATE_START = 0;
    public static final int STATE_CONNECTED = 1;
    public static final int STATE_LOGON_SENT = 2;
    public static final int STATE_LOGON_RECV = 4;
    public static final int STATE_RR_RECV = 8;
    public static final int STATE_RR_TIMEOUT = 16;
    public static final int STATE_LOGON = 31;

    private SessionOption sOpt;

    private ReentrantLock stateMutex;

    private int state;

    private Logger sLogger;

    private int fixVersionLen;
    private int bodyLengthPos;
    private byte[] msgSeq;

    private int msgTypePos;
    private int msgSeqPos;
    private int sendingTimePos;

    private int heartBTThreshold;
    private int testRequestThreshold;
    private int disconnectThreshold;

    private long lastSentUTCTS;
    private long lastRecvUTCTS;
    private int trSentFlag;

    private Clock clock;

    private Cmds cmds;

    private byte[] recvBuf;
    private int recvBufSize, recvBufHead, recvBufTail;
    
    private RecvQueue recvQueue;

    private SendQueue[] sendQueues;
    private int lastQIndexSent;
    
    private PManager pManager;
    private SentMark sentMarks;

    private FieldsParser sessionMsgsParser;

    private long numDataMsgsSent;
    private long numDataMsgsRecv;
    
    private Map<String, String> sProperties;

    protected abstract void readyForData();
    protected abstract void preProcessMsg(RMessage msg);
    protected abstract boolean onDataMsg(RMessage msg);
    protected abstract boolean onSessionMsg(RMessage msg);
    
    public Session(SessionOption opt) {
	sOpt = opt;
	sLogger = new SimpleLogger();
	try {
	    initSession();
	} catch (Exception e) {
	    e.printStackTrace();
	    sLogger.fatal(e.getMessage());
	}
    }

    public Session(SessionOption opt, Logger logger) {
	sOpt = opt;
	sLogger = logger;
	try {
	    initSession();
	} catch (Exception e) {
	    e.printStackTrace();
	    sLogger.fatal(e.getMessage());
	}
    }
    
    private void initSession() {
	clock = new Clock();
	stateMutex = new ReentrantLock();
    
	cmds = new Cmds();
	state = STATE_START;
	updateUTCTime();
    
	calculateMsgsPositions();

	recvQueue = new RecvQueue(sOpt.recvQueueSize, cmds, sLogger);
	
	sentMarks = new SentMark(Constants.SENT_MARK_SIZE);
	setMaxMsgSeqSize(sOpt.maxMsgSeqSize);
    
	long[] lastSeqs = new long[512];
	initPManager(lastSeqs);
        
	setHeartBTIn(sOpt.heartBTIn);
	
	sessionMsgsParser = new FieldsParser(Constants.MAX_FIX_TAG);
	
	initRecvBuf();

	sLogger.setLogFile(pManager.getSystemLog());
	
	initSendQueues();
	byte[] buf = new byte[512];
	for (int i = 1; i <= sOpt.numSendQueues; i++) {
	    String log = String.format("Queue #%d set initially lastSeqSent %d", i, lastSeqs[i]);
	    sLogger.warn(log);
	    sendQueues[i].markSeqSent(lastSeqs[i]);
	}
	numDataMsgsSent = 0;
	numDataMsgsRecv = 0;

	updateSendEventTS();
	updateRecvEventTS();
    }
    
        
    private void calculateMsgsPositions() {
	fixVersionLen = sOpt.fixVersion.length();
	bodyLengthPos = 5 + fixVersionLen;
	msgTypePos =  bodyLengthPos + sOpt.maxBodyLengthSize + 4;
	msgSeqPos = msgTypePos  + 5;
	sendingTimePos = msgSeqPos + sOpt.maxMsgSeqSize + 4 + sOpt.senderCompID.length() + 4 + sOpt.targetCompID.length() + 5 + 4; 
    }
    
    private int getMsgTypeSize(WMessage msg) {
	byte[] data = msg.getData();
	int dataStartPos = msg.getDataStartPos();
	int dataLength = msg.getDataLength();
	
	if (dataLength> 0) {
	    int msgTypeLen = Util.strSize(data, dataStartPos + msgTypePos);
	    return msgTypeLen;
	} else {
	    return -1;
	}
    }
    
    private void initPManager(long[] lastSeqs) {
	String sDir = String.format("%s-%s", sOpt.targetCompID, sOpt.senderCompID);
	String pDir = sOpt.pDir;
	if (pDir == null) pDir = DEFAULT_P_DIR;
	pManager = new PManager(sOpt.pDir, sDir, sOpt.numSendQueues + 1);
	recvQueue.setHeadSeq(pManager.readLastSeqProcessed());
	Arrays.fill(lastSeqs, 0);
	long lastSeq = pManager.readSentMarks(sentMarks, lastSeqs);
	SeqSentInfo info = null;
	if (lastSeq > 0) info = sentMarks.getMark(lastSeq);
	long seq;
	if (info == null) {
	    seq = 1;
	    state = STATE_RR_RECV;
	}  else {
	    seq = lastSeq + sOpt.windowMarkSize + 1;
	}
	Util.convertLTOA(seq, msgSeq, 0, sOpt.maxMsgSeqSize);
    }
    
    private void setQueue(int index, int size, int reserve) {
	FileHandler file = pManager.initDataQueue(index);
	FileHandler recoverFile = pManager.getDataRecoverFile(index);
	sendQueues[index] = new SendQueue(index, size, reserve,
					  sOpt.fixMsgAvgLength, sOpt.fixMsgMaxLength, file, recoverFile);
	pManager.close(recoverFile);
    }
    
    private void setMaxMsgSeqSize(int size) {
	msgSeq = new byte[size];
	Arrays.fill(msgSeq, 0, size - 1, (byte) '0');
	msgSeq[size - 1] = '1';
    }
    
    private void setHeartBTIn(int hbt) {
	heartBTThreshold = hbt;
	if (hbt == 0) {
	    hbt = SessionOption.DEFAULT_HB_TIME;
	}
	testRequestThreshold = (int) (Constants.TR_THRESHOLD*hbt);
	disconnectThreshold = (int) (Constants.DISCONNECT_THRESHOLD*hbt);
    }
    
    private void initSendQueues() {
	sendQueues = new SendQueue[sOpt.numSendQueues + 1];
	for (int i = 1; i <= sOpt.numSendQueues; i++) {
	    int qSize, qReserve;
	    if (sOpt.sendQueuesSize == null) {
		qSize = sOpt.recvQueueSize;
	    } else {
		qSize = sOpt.sendQueuesSize[i];
	    }
	    if (sOpt.sendQueuesReserve == null) {
		qReserve = qSize/8;
	    } else {
		qReserve = sOpt.sendQueuesReserve[i];
	    }
	    setQueue(i, qSize, qReserve);
	    boolean qFlag = ((sOpt.sendQueuesFlag == null) || (sOpt.sendQueuesFlag[i] == SendQueue.QUEUE_OPEN));
	    if (qFlag) {
		setQueueOpen(i);
	    } else {
		setQueueClose(i);
	    }
	}
    }
    
    private void initRecvBuf() {
	recvBufSize = sOpt.recvQueueSize*sOpt.fixMsgAvgLength;
	recvBuf = new byte[recvBufSize + Constants.SOCKET_RECV_WINDOWS];
	recvBufHead = 0;
	recvBufTail = 0;
    }
    
    private void processRecvQueueMsgs() {
	readyForData();
	FieldsParser parser = new FieldsParser(Constants.MAX_FIX_TAG);
	parser.setRGDictionary(sOpt.rgDictionary);
	while (true) {
	    RMessage msg = recvQueue.get();
	    msg.setParser(parser);
	    
	    processMsg(msg);
	    
	    pManager.writeSeqProcessed(msg.getSeq());
	    msg.reset();
	}
    }
    
    private void processMsg(RMessage msg) {
        if (msg.isGF()) return;

        while (true) {
            boolean code = onMsg(msg);
            if (code) {
                break;
            } else {
                Util.sleep(1);
            }
        }
    }

    private boolean onMsg(RMessage msg) {
	if (msg.isSession()) {
	    return onSessionMsg(msg);
	} else {
	    return onDataMsg(msg);
	}
    }

    protected boolean hasEvent() {
	for (int i = 1; i <= sOpt.numSendQueues; i++) {
	    if (sendQueues[i].isClose()) continue;
	    if (sendQueues[i].hasMsg()) return true;
	}
	return false;
    }
    
    public void start() {
	new Thread("AP-" + sOpt.sessionID) {
            public void run() {
                processRecvQueueMsgs();
            }
        }.start();
    }
    
    public Logger getLogger() {
	return sLogger;
    }

    public byte[] getRecvBuf() {
	return recvBuf;
    }
    
    public int getRecvBufHead() {
	return recvBufHead;
    }
    
    public int getRecvBufTail() {
        return recvBufTail;
    }

    public int getRecvBufSize() {
        return recvBufSize;
    }
    
    public void setRecvBuf(int head, int tail) {
	recvBufHead = head;
	recvBufTail = tail;
    }
  
    public boolean match(byte[] senderCompID, int senderCompIDOffset, 
			 byte[] targetCompID, int targetCompIDOffset) {
	return Util.match(senderCompID, senderCompIDOffset, sOpt.senderCompID) &&
	    Util.match(targetCompID, targetCompIDOffset, sOpt.targetCompID);
    }
  
    public void updateUTCTime() {
	clock.updateUTCTime();
    }
    
    public int getNumQueues() {
	return sOpt.numSendQueues;
    }
  
    public void markMsgSent(WMessage msg) {
	sendQueues[msg.getQIndex()].markMsgSent(msg);
	numDataMsgsSent++;
    }

    public boolean isQueueAvailable(int index) {
	if (index > sOpt.numSendQueues || index <= 0) return false;
	if (sendQueues[index].isClose()) return false;
	return true;
    }

    public void setQueueOpen(int index) {
	if (index > sOpt.numSendQueues || index <= 0) return;
	sendQueues[index].setOpen();
	signal();
	String log = String.format("Queue #%d opened", index);
	sLogger.warn(log);
    }
    
    public void setQueueClose(int index) {
	if (index > sOpt.numSendQueues || index <= 0) return;
	sendQueues[index].setClose();
	String log = String.format("Queue #%d closed", index);
	sLogger.warn(log);
    }
  
    public void setQueueFlagRef(int index, Flag flag) {
	sendQueues[index].setQFlagRef(flag);
    }

    public Flag getQueueFlagRef(int index) {
	return sendQueues[index].getQFlagRef();
    }
    
    public void setQueueFlagThreshold(int index, int threshold) {
	sendQueues[index].setQFlagThreshold(threshold);
    }

    public void updateSeq(long currentSeqNo, long newSeqNo) {
	recvQueue.updateSeq(currentSeqNo, newSeqNo);
    }
    
    public byte[] getNextCmd(byte[] cmd) {
	return cmds.getNextCmd(cmd);
    }
    
    public void updateRecvEventTS() {
	lastRecvUTCTS = clock.getTimestamp();
	trSentFlag = 0;
    }
    
    public void updateSendEventTS() {
	lastSentUTCTS = clock.getTimestamp();
    }
    
    public boolean isRecvTimeout() {
	int elapseTime = (int) (clock.getTimestamp() - lastRecvUTCTS);
	
	if (elapseTime < testRequestThreshold){
	    return false;
	}
	
	if (elapseTime >= disconnectThreshold) {
	    lastRecvUTCTS = clock.getTimestamp();
	    return true;
	} else {
	    if (trSentFlag == 0) {
		addTRCmd();
		trSentFlag = 1;
	    }
	    return false;
	}
    }
    
    public boolean isSendTimeout() {
	int elapseTime = (int) (clock.getTimestamp() - lastSentUTCTS);
      
	if (heartBTThreshold != 0 && elapseTime >= heartBTThreshold) {
	    lastSentUTCTS = clock.getTimestamp();
	    return true;
	} 
	return false;
    }

    public void addHBCmd(String id) {
	byte[] cmd = new byte[Constants.CMD_BLOCK_SIZE];
	cmd[0] = (byte) Constants.MSG_TYPE_HEARTBEAT.charAt(0);
	
	int length = (id == null)?1:id.length() + 1;
	
	if (length > Constants.CMD_BLOCK_SIZE) {
	    length = Constants.CMD_BLOCK_SIZE;
	}
		
	for (int i = 1; i < length; i++) {
	    cmd[i] = (byte) id.charAt(i - 1);
	}

	cmd[length - 1] = 0;

	cmds.addCmd(cmd);
    }

    public void addTRCmd() {
	byte[] cmd = new byte[Constants.CMD_BLOCK_SIZE];
	cmd[0] = (byte) Constants.MSG_TYPE_TEST_REQUEST.charAt(0);
	cmds.addCmd(cmd);
    }

    public void addSequenceResetCmd(long beginSeqNo, long endSeqNo) {
	byte[] cmd = new byte[Constants.CMD_BLOCK_SIZE];
	cmd[0] = (byte) Constants.MSG_TYPE_SEQUENCE_RESET.charAt(0);
	Util.convertToBytes(beginSeqNo, cmd, 1);
	Util.convertToBytes(endSeqNo, cmd, 1 + Constants.LONG_BYTES);
	cmds.addCmd(cmd);
    }
  
    public void lockAllSendQueues() {
	for (int i = 1; i <= getNumQueues(); i++) {
	    sendQueues[i].lock();
	}
    }

    public void unlockAllSendQueues() {
	for (int i = 1; i <= getNumQueues(); i++) {
	    sendQueues[i].unlock();
	}
    }
    
    
    public int getSentMsg(WMessage msg, long seq) {
    
	long dataSeq;
    
	SeqSentInfo info = sentMarks.getMark(seq);
	if (info == null) {
	    info = sentMarks.getLastMark(seq);
	    dataSeq = info.dataSeq + seq - info.msgSeq;
	} else {
	    if (info.msgSeq != seq) return 0;
	    dataSeq = info.dataSeq;
	}
	
	if (info.qIndex == Constants.QUEUE_SESSION) return 0;
	int code = sendQueues[info.qIndex].getSentMsg(msg, dataSeq);
	return code;
    }

    public void getMsg(WMessage msg, String msgType) {
	
	if (msg.getData() == null) {
	    int qIndex = msg.getQIndex();
	    msg.switchData(sendQueues[qIndex].getNewMsgBuf(), 
			   sendQueues[qIndex].getNewMsgPos(), 
			   0);
	}
	
	msg.setMaxBodyLengthSize(sOpt.maxBodyLengthSize);
	
	msg.markBodyLengthPos(bodyLengthPos);
	msg.markBodyEndPos(bodyLengthPos + sOpt.maxBodyLengthSize);
	msg.setField(Constants.BEGIN_STRING, sOpt.fixVersion);
	
	msg.reserveField(Constants.BODY_LENGTH, sOpt.maxBodyLengthSize);
	msg.setField(Constants.MSG_TYPE, msgType);
	msg.reserveField(Constants.MSG_SEQ_NUM, sOpt.maxMsgSeqSize);
	
	msg.setField(Constants.SENDER_COMP_ID, sOpt.senderCompID);

	msg.setField(Constants.TARGET_COMP_ID, sOpt.targetCompID);
	msg.setField(Constants.POS_DUP_FLAG, 'N');
	msg.reserveField(Constants.SENDING_TIME, Constants.SENDING_TIME_SIZE);  
	msg.reserveField(Constants.ORIG_SENDING_TIME, Constants.SENDING_TIME_SIZE);
        
	for (int i = 0; i < sOpt.numHeaderOptions; i++) {
	    msg.setField(sOpt.headerOptionsTag[i], sOpt.headerOptionsVal[i]);
	}
	
	if (msgType.length() == 1) {
	    if (msgType.charAt(0) == Constants.MSG_TYPE_LOGON.charAt(0)) {
		msg.setField(Constants.ENCRYPT_METHOD, Constants.ENCRYPT_METHOD_NONE);
		msg.setField(Constants.HEART_BT_IN, sOpt.heartBTIn);
		if (sOpt.defaultApplVerID >= 0) {
		    msg.setField(Constants.DEFAULT_APPL_VER_ID, sOpt.defaultApplVerID);
		}

		for (int i = 0; i < sOpt.numLogonOptions; i++) {
		    msg.setField(sOpt.logonOptionsTag[i], sOpt.logonOptionsVal[i]);
		}

	    } else if (msgType.charAt(0) == Constants.MSG_TYPE_TEST_REQUEST.charAt(0)) {
		msg.setField(Constants.TEST_REQ_ID, msgSeq, 0, sOpt.maxMsgSeqSize);
	    }
	}

    }
    
    public int addMsg(WMessage msg, int numRetries) {
	int code = sendQueues[msg.getQIndex()].addMsg(msg, numRetries);
	if (code > 0) signal();
	return code;
    }

    public int addMsg(WMessage msg) {
	return addMsg(msg, 0);
    }
  
    public int getQueuedMsg(WMessage msg) {
	return sendQueues[msg.getQIndex()].getMsg(msg);
    }
    
    public int getState() {
	return state;
    }
    
    public boolean isLogon() {
	return state == STATE_LOGON;
    }

    public int getStateDescription(byte[] buf, int offset) {
	if (state == STATE_LOGON) {
	    return Util.convertToBytes("LOGON", buf, offset);
	}
	if (!isState(STATE_CONNECTED)) {
	    return Util.convertToBytes("DISCONNECTED", buf, offset);
	}
	
	if (!isState(STATE_LOGON_SENT) && !isState(STATE_LOGON_RECV)) {
	    return Util.convertToBytes("CONNECTED", buf, offset);
	}
	
	if (!isState(STATE_RR_RECV) || !isState(STATE_RR_TIMEOUT)) {
	    return Util.convertToBytes("SESSION INIT-TRY LOGON", buf, offset);
	}
	
	return Util.convertToBytes("SESSION INIT-RECOVERING", buf, offset);
    }
    
    public void addState(int addState) {
	stateMutex.lock();
	if (isState(addState)) {
	    stateMutex.unlock(); 
	    return;
	}
	
	if (addState == STATE_CONNECTED || addState == STATE_RR_RECV || isState(STATE_CONNECTED)) {
	    state += addState;
	} 
	String log = String.format("Current session state is %d", state);  
	sLogger.warn(log);
	stateMutex.unlock();
    }
    
    public boolean isState(int stateToCheck) {
	return state%(2*stateToCheck) >= stateToCheck;  
    }
    
    public void resetState() {
	stateMutex.lock();
	state = STATE_RR_RECV;
	stateMutex.unlock();
    }

    public long getQNumPendingMsgs(int qIndex) {
	return sendQueues[qIndex].getNumPendingMsgs();
    }
    
    public int getNumSendQueues() {
	return sOpt.numSendQueues;
    }
  
    public long getNumDataMsgsSent() {
	return numDataMsgsSent;
    }
    
    public long getNumDataMsgsRecv() {
	return numDataMsgsRecv;
    }
    
    public String getSessionID() {
	return sOpt.sessionID;
    }
    
    public int getFIXVersionLen() {
	return fixVersionLen;
    }
    
    public final long getAndIncreaseTailSeq() {
	return  recvQueue.getAndIncreaseTailSeq();
    }

    public long getMsgSeq() {
	return Util.convertATOL(msgSeq, 0, sOpt.maxMsgSeqSize);
    }
    
    public final void setProperty(String key, String value) {
        if (sProperties == null) {
            sProperties = new HashMap<String, String>();
        }
        sProperties.put(key, value);
    }

    public final String getProperty(String key) {
        return sProperties.get(key);
    }

    public void putMsgToAppQueue(RMessage msg, boolean isSessionMsg) {
	preProcessMsg(msg);
	recvQueue.put(msg);
	if (!isSessionMsg) {
	    numDataMsgsRecv++;
	}
    }
    
    public FieldsParser getSessionMsgsParser() {
	return sessionMsgsParser;
    }
    
    public void incrementMsgSeqNum() {
	int index = sOpt.maxMsgSeqSize - 1;
	while (true) {
	    if (msgSeq[index] == (byte) '9') {
		msgSeq[index] = (byte) '0';
		index--;
	    } else {
		msgSeq[index]++;
		break;
	    }
	}
    }
    
    public void finalizeMsg(WMessage msg) {
	int add = getMsgTypeSize(msg) - 1;
	if (add >= 0) {
	    msg.finalize(msgSeqPos + add, msgSeq, sOpt.maxMsgSeqSize, 
			 sendingTimePos + add, clock.getUTCTime(), false);
	} else {
	    String log = String.format("Get msg size 0 at seq %d, data_seq %d, head_data_seq %d", 
				       Util.convertATOL(msgSeq, 0, sOpt.maxMsgSeqSize), 
				       msg.getDataSeq(), msg.indexSeq);
	    sLogger.fatal(log);
	}
    }
    
    public void finalizeMsg(WMessage msg, long seq, boolean resendFlag) {
	byte[] buf = new byte[32];
	int numDigits = Util.convertLTOA(seq, buf, 0);
	byte[] seqBuf = new byte[32];
	Arrays.fill(seqBuf, 0, sOpt.maxMsgSeqSize - numDigits, (byte) '0');
	System.arraycopy(buf, 0, seqBuf, sOpt.maxMsgSeqSize - numDigits, numDigits); 
	int add = getMsgTypeSize(msg) - 1;
	msg.finalize(msgSeqPos + add, seqBuf, sOpt.maxMsgSeqSize, 
		     sendingTimePos + add, clock.getUTCTime(), resendFlag);
    }
    
    public void writeMsgSent(WMessage msg, int flag) {
	if (flag == 0) return;
	
	boolean wFlag = false;
	
	int qIndex = msg.getQIndex();
        
	long msgSeq = msg.getSeq();
	
	long dataSeq = msg.getDataSeq();
	
	if (qIndex != lastQIndexSent) {
	    wFlag = true;
	    lastQIndexSent = qIndex;
	} else if (qIndex == 0 || msgSeq%sOpt.windowMarkSize == 0) {
	    wFlag = true;
	}
    
	if (wFlag) {
	    pManager.writeSeqSent(qIndex, msgSeq, dataSeq);
	    sentMarks.setMark(qIndex, msgSeq, dataSeq);
	} 
    }
  
}
