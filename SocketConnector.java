package jufix;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

import java.net.Socket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;

public class SocketConnector {
    
    private static final int SOCKET_TYPE_ACCEPTOR = 1;
    private static final int SOCKET_TYPE_INITIATOR = 2;
    
    private static final int THREAD_STATUS_START = 1;
    private static final int THREAD_STATUS_STOP = 0;
    
    private static final int SOCKET_BLOCK_TIME = 1000;
    
    private static final int  RR_TIMEOUT = 5;

    private int flag;
    private int socketType;
    private Socket socket;
    private String socketAddr;
    private int socketPort;
    private InputStream sis;
    private OutputStream sos;

    private int socketSendFlag;
    
    private Thread threadRecv, threadSend, threadTimer;

    private Session session;

    private byte[] recvBuf;
    private int recvBufHead, recvBufTail, recvBufSize;
    
    public SocketConnector(byte[] tempBuf, int tempBufCur, int length, Socket socket, Session s) throws IOException {
	session = s;
	recvBuf = session.getRecvBuf();
	recvBufHead = session.getRecvBufHead();
	recvBufTail = session.getRecvBufTail(); 
	recvBufSize = session.getRecvBufSize();
	System.arraycopy(tempBuf, tempBufCur, recvBuf, recvBufTail, length);
	recvBufTail += length;
	
	this.socket = socket;
	sis = socket.getInputStream();
	sos = socket.getOutputStream();
	socketType = SOCKET_TYPE_ACCEPTOR;
	session.addState(Session.STATE_CONNECTED);
	socketInit();
    }
  
    public SocketConnector(Session s) {
	session = s;
	recvBuf = session.getRecvBuf();
	recvBufHead = session.getRecvBufHead();
	recvBufTail = session.getRecvBufTail();
	recvBufSize = session.getRecvBufSize();
	socket = null;
	sis = null;
	sos = null;
	socketType = SOCKET_TYPE_INITIATOR;
    }
  
    private void logFatal(String msg) {
	Logger logger = session.getLogger();
	if (logger != null) {
	    logger.fatal(msg);
	} 
    }

    private void logError(String msg) {
	Logger logger = session.getLogger();
	if (logger != null) {
	    logger.error(msg);
	} 
    }

    private void logWarn(String msg) {
	Logger logger = session.getLogger();
	if (logger != null) {
	    logger.warn(msg);
	} 
    }

    private void logInfo(String msg) {
	Logger logger = session.getLogger();
	if (logger != null) {
	    logger.info(msg);
	}
    }
  
    private void logDataMsgSent(WMessage msg) {
	Logger logger = session.getLogger();
	if (logger != null) {
	    logger.dataMsgSent(msg);
	}           
    }    

    private void logDataMsgRecv(RMessage msg) {
	Logger logger = session.getLogger();
	if (logger != null) {
	    logger.dataMsgRecv(msg);
	}
    }

    private void logSessionMsgSent(WMessage msg) {
	Logger logger = session.getLogger();
	if (logger != null) {
	    logger.sessionMsgSent(msg);
	}
    }

    private void logSessionMsgRecv(RMessage msg) {
	Logger logger = session.getLogger();
	if (logger != null) {
	    logger.sessionMsgRecv(msg);
	} 
    }

    private void logMsgError(byte[] msg, int msgStartPos, int msgLength, String error) {
	Logger logger = session.getLogger();
	if (logger != null) {
	    logger.msgError(msg, msgStartPos, msgLength, error);
	} 
    }
    
    private void socketReset() throws IOException {
	socketClose();
	socketCreate();
	socketInit();
    }
    
    private void socketClose() {
	if (socket != null) {
            try {
                socket.close();
            } catch (IOException ie) {
            }
	    socket = null;
        }
    }

    private void socketCreate() throws IOException {
	socket = new Socket();
    }

    private void socketInit() throws IOException {
	socket.setTcpNoDelay(true);
	socket.setSoLinger(true, 0);
	socket.setReceiveBufferSize(Constants.SOCKET_RECV_BUF_SIZE);
	socket.setSendBufferSize(Constants.SOCKET_SEND_BUF_SIZE);
	socket.setSoTimeout(SOCKET_BLOCK_TIME);
	socketSendFlag = 1;
    }
    
    private void socketConnect() {
	
	while (true) {
	    try {
		socketReset();
		String log = String.format("Connection to fix server %s:%d initiating", socketAddr, socketPort);  
		logWarn(log);
		socket.connect(new InetSocketAddress(socketAddr, socketPort));
		sis = socket.getInputStream();     
		sos = socket.getOutputStream();  
		break;
	    } catch (SocketTimeoutException se) {
		String log = String.format("Connection to fix server %s:%d timeout. Error msg: %s", 
					   socketAddr, socketPort, se.getMessage());
		logError(log);
		Util.sleep(10000);
	    } catch (IOException ie) {
		String log = String.format("Connection to fix server %s:%d failed. Error msg: %s",
					   socketAddr, socketPort, ie.getMessage());
		logError(log);
		Util.sleep(10000);
	    }
	} 
	String log = String.format("Connection to fix server %s established", socketAddr);
	logInfo(log);
    }
  
    public void setSocketAddr(String addr) {
	socketAddr = addr;
    }

    public void setSocketPort(int port) {
	socketPort = port;
    }
  
    public void join() throws InterruptedException {
	threadSend.join();
	threadRecv.join();
    }

    public void start() {
	flag = THREAD_STATUS_START;

	updateUTCTime();
	
	if (socketType == SOCKET_TYPE_INITIATOR) {
	    threadTimer = new Thread("TM-" + session.getSessionID()) {
		    public void run() {
			logWarn("Timer thread is running");
			keepUpdateUTCTime();
			logWarn("Timer thread exit");
		    }
		};
	    threadTimer.start();
	}
	
	threadRecv = new Thread("RC-" + session.getSessionID()) {
		public void run() {
		    logWarn("Receiver thread is running");     
		    recvMsgs();
		    logWarn("Receiver thread exit");
		}
	    };
	threadRecv.start();
	
	threadSend = new Thread("SD-" + session.getSessionID()) {
		public void run() {
		    logWarn("Sender thread is running");
		    sendMsgs();
		    logWarn("Sender thread exit");
		}
	    };
	threadSend.start();
    }
    
    private void updateUTCTime() {
	session.updateUTCTime();
    }

    public void keepUpdateUTCTime() {
	while (flag == THREAD_STATUS_START) {
	    session.updateUTCTime();
	    Util.sleep(1000);
	}
    }
    
    private int read(InputStream is, byte[] buf, int offset) {
        try {
            return is.read(buf, offset, Constants.SOCKET_RECV_WINDOWS);
        } catch (SocketTimeoutException se) {
            return 0;
        } catch (IOException ie) {
            logError(ie.getMessage());
	    return -1;
        }
    }

    private void recvMsgs() {
	if (recvBufTail > recvBufHead) {
	    processNewData();
	}
	
	while (flag == THREAD_STATUS_START) {
	    if (!session.isState(Session.STATE_CONNECTED)) {
		Util.sleep(1000);
		continue;
	    }
	    
	    int len = read(sis, recvBuf, recvBufTail);
	    if (len > 0) {
		recvBufTail += len;
		if (processNewData()) {
		    if (recvBufTail > recvBufSize) {
			System.arraycopy(recvBuf, recvBufHead, recvBuf, 0, recvBufTail - recvBufHead);
			recvBufTail -= recvBufHead;
			recvBufHead = 0;		
		    }
		    continue;
		} 
	    }
	    
	    if (len == 0 && !session.isRecvTimeout()) {
		continue;
	    }

	    logWarn("Session disconnected");
	    session.resetState();
	    recvBufTail = recvBufHead;
            
	    if (socketType == SOCKET_TYPE_ACCEPTOR) {
		flag = THREAD_STATUS_STOP;
		session.setRecvBuf(recvBufHead, recvBufTail);
	    } 
	}
    }
  
    private boolean processNewData() {
	while (true) {
	    if (recvBufTail - recvBufHead < 12) return true;
	    int pos = recvBufHead + session.getFIXVersionLen() + 2;
	    while (true) {
		if (pos > recvBufTail) return true;
		if (recvBuf[pos] == Constants.SOH) break;
		pos++;
	    }
	    pos += 3;
	    int msgLenPos = pos;
	    
	    while (true) {
		if (pos > recvBufTail) return true;
		if (recvBuf[pos] == Constants.SOH) break;
		pos++;
	    }
	    
	    int msgTypePos = pos + 4;
	    
	    int msgLen = pos + 1 - recvBufHead + Util.convertATOI(recvBuf, msgLenPos) + Constants.CHECKSUM_SIZE;
	    if (recvBufTail - recvBufHead < msgLen) return true;
	    
	    int seqPos = Util.seek(recvBuf, msgTypePos, msgLen - (msgTypePos - recvBufHead),
				   Constants.SEQ_PATTERN, Constants.SEQ_PATTERN_LENGTH);
	    long seq = Util.convertATOL(recvBuf, seqPos + Constants.SEQ_PATTERN_LENGTH);
	    
	    RMessage msg = new RMessage(recvBuf, recvBufHead, msgLen, msgTypePos - recvBufHead, seq); 
	    processMsg(msg);
	    recvBufHead += msgLen;
	}
    }
  
    private void processMsg(RMessage msg) {
	session.updateRecvEventTS();
		
	boolean isSessionMsg = msg.isSession();
	
	if (isSessionMsg) {
	    logSessionMsgRecv(msg);
	    msg.setParser(session.getSessionMsgsParser());
	    processSessionMsg(msg);
	} else {
	    logDataMsgRecv(msg);
	}
	session.putMsgToAppQueue(msg, isSessionMsg);
    }
    
    private void processSessionMsg(RMessage msg) {
	msg.parse();
	String msgType = msg.getMsgType();
	
	char type = msgType.charAt(0);
	
	if (type == Constants.MSG_TYPE_HEARTBEAT.charAt(0)) {
	    processMsgHeartbeat(msg); 
	} else if (type == Constants.MSG_TYPE_TEST_REQUEST.charAt(0)) {
	    processMsgTestRequest(msg);
	} else if (type == Constants.MSG_TYPE_RESEND_REQUEST.charAt(0)) {
	    processMsgResendRequest(msg);
	} else if (type == Constants.MSG_TYPE_REJECT.charAt(0)) {
	    processMsgReject(msg);
	} else if (type == Constants.MSG_TYPE_SEQUENCE_RESET.charAt(0)) {
	    processMsgSequenceReset(msg);
	} else if (type == Constants.MSG_TYPE_LOGOUT.charAt(0)) {
	    processMsgLogout(msg);
	} else if (type == Constants.MSG_TYPE_LOGON.charAt(0)) {
	    processMsgLogon(msg);
	}
    }

    private void processMsgHeartbeat(RMessage msg) {
    }

    private void processMsgTestRequest(RMessage msg) {
	String reqID = msg.getString(Constants.TEST_REQ_ID);
	session.addHBCmd(reqID);
    }

    private void processMsgResendRequest(RMessage msg) {
	long beginSeqNo = msg.getLong(Constants.BEGIN_SEQ_NO);
	long endSeqNo = msg.getLong(Constants.END_SEQ_NO);
	session.addSequenceResetCmd(beginSeqNo, endSeqNo);
	session.addState(Session.STATE_RR_RECV);
    }
    
    private void processMsgReject(RMessage msg) {
    }

    private void processMsgSequenceReset(RMessage msg) {
	long currentSeq = msg.getSeq();
	long newSeqNo = msg.getLong(Constants.NEW_SEQ_NO);
	session.updateSeq(currentSeq, newSeqNo);
    }

    private void processMsgLogout(RMessage msg) {
    }

    private void processMsgLogon(RMessage msg) {
	session.addState(Session.STATE_LOGON_RECV);
    }

    private void sendMsgs() {
	int sendStatus = 1;
	long rrRecvTS = 0;
	
	while (flag == THREAD_STATUS_START) {
	    if (sendStatus == 0) {
		session.wait(SOCKET_BLOCK_TIME, !session.isLogon());
	    }
	    
	    if (socketType == SOCKET_TYPE_INITIATOR && (!session.isState(Session.STATE_CONNECTED))) {
		logWarn("State not connected");
		sendStatus = 0;
		socketConnect();
		session.addState(Session.STATE_CONNECTED);
		rrRecvTS = 0;
		continue;
	    }
	    
	    if (socketSendFlag == 0) {
		sendStatus = 0;
		continue;
	    }

	    if (!session.isState(Session.STATE_LOGON_SENT)) {
		if (socketType == SOCKET_TYPE_INITIATOR || session.isState(Session.STATE_LOGON_RECV)) {
		    if (sendLogon() > 0) {
			session.addState(Session.STATE_LOGON_SENT);
		    } 
		}
		sendStatus = 0;
		continue;
	    }
	    
	    if (!session.isState(Session.STATE_LOGON_RECV)) {
		sendStatus = 0;
		continue;
	    }
	    	    
	    sendCmds();
	    
	    if (!session.isLogon()) {
		if (session.isState(Session.STATE_RR_RECV) && !session.isState(Session.STATE_RR_TIMEOUT)) {
		    if (rrRecvTS == 0) {
			rrRecvTS = Util.getCurrentTimestamp();
		    } else {
			if ((int) (Util.getCurrentTimestamp() - rrRecvTS) > RR_TIMEOUT*Constants.MILLION) {
			    session.addState(Session.STATE_RR_TIMEOUT);
			}
		    }
		}
		sendStatus = 0;
		continue;
	    }

	    sendStatus = sendData();
	    if (sendStatus == 0) {
		if (session.isSendTimeout()) {
		    byte[] cmd = new byte[8];
		    cmd[1] = 0;
		    sendStatus = sendHeartbeat(cmd);
		}
	    } 
	    
	    if (sendStatus != 0) {
		session.updateSendEventTS();
	    }
	}
    }
  
    private void sendCmds() {
	byte[] cmd = new byte[Constants.CMD_BLOCK_SIZE];
	while (true) {
	    if (session.getNextCmd(cmd) == null) break;
	    sendCmd(cmd);
	}
    }
    
    private Exception send(byte[] data, int offset, int len) {
        try {
	    sos.write(data, offset, len);
	    sos.flush();
	    return null;
        } catch (IOException ie) {
	    return ie;
        }
    }

    private int sendMsg(WMessage msg, int newMsgFlag, int dataFlag) {
    
	byte[] data = msg.getData();
	int msgStartPos = msg.getDataStartPos();
	int msgLength = msg.getDataLength();
	        
	session.writeMsgSent(msg, newMsgFlag);
    
	int code;

	Exception sendException = send(data, msgStartPos, msgLength);
	
	if (sendException != null) {
	    String log = String.format("Send msg seq %d error. Error code %s", 
				       msg.getSeq(), sendException.getMessage());
	    logWarn(log);
	    socketSendFlag = 0;
	    if (socketType == SOCKET_TYPE_ACCEPTOR) {
		flag = THREAD_STATUS_STOP;
	    }
	    return 0;
	}
	
	if (dataFlag > 0) {
	    logDataMsgSent(msg);
	} else {
	    logSessionMsgSent(msg);
	}
	if (newMsgFlag > 0) {
	    session.incrementMsgSeqNum();
	}
	return 1;
    }
  
    private int sendData() {
	int numQueues = session.getNumQueues();
	int numMsgsSent = 0;
	
	for (int i = 1; i <= numQueues; i++) {
	    
	    if (!session.isQueueAvailable(i)) {
		continue;
	    }
      
	    WMessage msg = new WMessage(i);
	    int qMsgsSent = 0;
      
	    while (qMsgsSent < Constants.WINDOW_QUEUE_RR_SIZE) {
		int msgCode = session.getQueuedMsg(msg);
		if (msgCode == 0) break;
		session.finalizeMsg(msg);
		sendMsg(msg, 1, 1);
		session.markMsgSent(msg);
		if (socketSendFlag == 0) break;
		qMsgsSent++;
	    }
	    numMsgsSent += qMsgsSent;
	}
	return numMsgsSent;
    }

    private int sendCmd(byte[] cmd) {
	String log = String.format("Sending cmd to other side %c", cmd[0]);
	logWarn(log);
	char cmdType = (char) cmd[0]; 
	if (cmdType == Constants.MSG_TYPE_HEARTBEAT.charAt(0)) {
	    return sendHeartbeat(cmd);
	} 
	
	if (cmdType == Constants.MSG_TYPE_TEST_REQUEST.charAt(0)) {
	    return sendTestRequest();
	}
	
	if (cmdType == Constants.MSG_TYPE_RESEND_REQUEST.charAt(0)) {
	    return sendResendRequest(cmd);
	}

	if (cmdType == Constants.MSG_TYPE_SEQUENCE_RESET.charAt(0)) {
	    return sendSequenceReset(cmd);
	}
	
	logWarn("Cmd type not supported: " + cmdType);
	return 0;
    }

    private int sendLogon() {
	byte[] buf = new byte[1024];
	WMessage msg = new WMessage(Constants.QUEUE_SESSION, buf, 0);
	session.getMsg(msg, Constants.MSG_TYPE_LOGON);
	session.finalizeMsg(msg);
	return sendMsg(msg, 1, 0);
    }

    private int sendHeartbeat(byte[] cmd) {
	byte[] buf = new byte[1024];
	WMessage msg = new WMessage(Constants.QUEUE_SESSION, buf, 0);
	session.getMsg(msg, Constants.MSG_TYPE_HEARTBEAT);
	if (cmd[1] != 0) {
	    msg.setField(Constants.TEST_REQ_ID, Util.convertToString(cmd, 1));
	}
	session.finalizeMsg(msg);
	return sendMsg(msg, 1, 0);
    }

    private int sendTestRequest() {
	byte[] buf = new byte[1024];
	WMessage msg = new WMessage(Constants.QUEUE_SESSION, buf, 0);
	session.getMsg(msg, Constants.MSG_TYPE_TEST_REQUEST);
	session.finalizeMsg(msg);
	return sendMsg(msg, 1, 0);
    }

    private int sendResendRequest(byte[] cmd) {
	byte[] buf = new byte[1024];
	WMessage msg = new WMessage(Constants.QUEUE_SESSION, buf, 0);
	session.getMsg(msg, Constants.MSG_TYPE_RESEND_REQUEST);
	msg.setField(Constants.BEGIN_SEQ_NO, Util.convertToLong(cmd, 1));
	msg.setField(Constants.END_SEQ_NO, Util.convertToLong(cmd, 1 + Constants.LONG_BYTES));
	session.finalizeMsg(msg);
	return sendMsg(msg, 1, 0);
    }

    private int sendSequenceReset(long start, long end) {
	byte[] buf = new byte[1024];
	WMessage msg = new WMessage(Constants.QUEUE_SESSION, buf, 0);
	session.getMsg(msg, Constants.MSG_TYPE_SEQUENCE_RESET);
	msg.setField(Constants.GAP_FILL_FLAG, 'Y');
	msg.setField(Constants.NEW_SEQ_NO, end);
	session.finalizeMsg(msg, start, false);
	return sendMsg(msg, 0, 0);
    }

    private int sendSequenceReset(byte[] cmd) {
	String log = String.format("Process resend request from other side");
        logWarn(log);

	long beginSeqNo = Util.convertToLong(cmd, 1);
	long endSeqNo = Util.convertToLong(cmd, 1 + Constants.LONG_BYTES);
	if (endSeqNo == 0) {
	    endSeqNo = session.getMsgSeq();
	} else {
	    endSeqNo++;
	}
	long seq = beginSeqNo;
	int numMsgsSent = 0;
    
	long startResetSeq = beginSeqNo;
    
	log = String.format("Trying to resend msgs seq from %d to %d ", beginSeqNo, endSeqNo);
	logWarn(log);
    
	session.lockAllSendQueues();
	logWarn("All send queues locked");
	while (true) {
	    WMessage sentMsg = new WMessage(Constants.QUEUE_SESSION); 
      
	    int code = session.getSentMsg(sentMsg, seq);
	    int sendCode;

	    if (code > 0) {
		if (startResetSeq < seq) {
		    sendCode = sendSequenceReset(startResetSeq, seq);
		    if (sendCode == 0) {
			log = String.format("Send msg sequence reset seq %d error", 
					    seq);
			logWarn(log);
			break;
		    }
		    numMsgsSent += sendCode;
		    startResetSeq = seq;
		}
		session.finalizeMsg(sentMsg, seq, true);
		sendCode = sendMsg(sentMsg, 0, 0);
		if (sendCode == 0) {
		    log = String.format("Send data msg seq %d error", seq);
		    logWarn(log);
		    break;
		}
		numMsgsSent += sendCode;
		startResetSeq++;
	    }
	    seq++;
	    if (seq == endSeqNo) {
		if (startResetSeq < seq - 1) {
		    sendCode = sendSequenceReset(startResetSeq, seq);
		    numMsgsSent += sendCode;
		}
		logWarn("Resend msgs completed sucessfully");
		break;
	    }
	}
	session.unlockAllSendQueues();
	logWarn("All send queues unlocked");
	return numMsgsSent;
    }
}

