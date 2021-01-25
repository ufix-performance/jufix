package jufix;

class RecvQueue extends Locker {
    
    private Logger qLogger;
    
    private Cmds cmds;
    
    private RMessage[] msgsRecv;
    private int qSize;

    private int head;
    private long headSeq;
    private volatile long tailSeq;

    public RecvQueue(int size, Cmds reqs, Logger logger) {
	this.qSize = size;
	this.cmds = reqs;
	this.qLogger = logger;
	this.msgsRecv = new RMessage[qSize];
	
	for (int i = 0; i < qSize;  i++) {
	    msgsRecv[i] = new RMessage();
	}

	head = 1;
	headSeq = 0;
	tailSeq = 0;
    }
    
    private boolean addResendRequest() {
	long beginSeqNo = headSeq + 1;
	int index = Util.getQueueIndex(beginSeqNo, qSize);
	if (msgsRecv[index].getTimestamp() > 0) return false;

	long timestamp = Util.getCurrentTimestamp();
	msgsRecv[index].setTimestamp(timestamp);
	
	long endSeqNo = beginSeqNo + 1;

	while (true) {
	    index = Util.getQueueIndex(endSeqNo, qSize);
	    if (msgsRecv[index].isEmpty()) {
		msgsRecv[index].setTimestamp(timestamp);
		endSeqNo++;
	    } else {
		break;
	    }
	}
	endSeqNo--;

	byte[] cmd = new byte[Constants.CMD_BLOCK_SIZE];
	cmd[0] = (byte) Constants.MSG_TYPE_RESEND_REQUEST.charAt(0);
	
	Util.convertToBytes(beginSeqNo, cmd, 1);
	Util.convertToBytes(endSeqNo, cmd, 1 + Long.BYTES);
	cmds.addCmd(cmd);
	return true;
    }

    
    protected boolean hasEvent() {
	return !(msgsRecv[head].isEmpty());
    }
    
    public void setHeadSeq(long seq) {
	head = Util.getQueueIndex(seq + 1, qSize);
	headSeq = seq;
	tailSeq = seq;
    }
    
    public void updateSeq(long currentSeqNo, long newSeqNo) {  
	for(long i = currentSeqNo + 1; i < newSeqNo; i++) {
	    msgsRecv[Util.getQueueIndex(i, qSize)].setGF(i);
	}
    }
    
    public long getAndIncreaseTailSeq() {
        return tailSeq++;
    }

    public void put(RMessage msg) {
	
	long seq = msg.getSeq();
	int index = Util.getQueueIndex(seq, qSize);
	
	long slotSeq = msgsRecv[index].getSeq();
	
	if (slotSeq >= seq) {
	    String log = String.format("Recv seq %d while already recv seq %d. Discard msg", 
				       seq, slotSeq);
	    qLogger.warn(log);
	    return;
	}
	long startTS = 0;
	
	while (!msgsRecv[index].isEmpty()) {
	    if (startTS == 0) startTS = Util.getCurrentTimestamp();
	    Util.sleep(1);
	    long currentTS = Util.getCurrentTimestamp();
	    if (currentTS > startTS + 5*Constants.MILLION) {
		startTS = currentTS;
		String log = String.format("Recv queue add msg seq %d to index %d failed. Queue full. Slot seq: %d", 
					   msg.getSeq(), index, msgsRecv[index].getSeq());
		qLogger.warn(log);
	    }  
	}

	msgsRecv[index].copy(msg);
	if (tailSeq < seq) {
	    tailSeq = seq;
	}
	signal();
    }
  
    public RMessage get() {
	long startTS = 0;
	
	while (msgsRecv[head].isEmpty()) {
	    wait(1000, false);
	    
	    if (tailSeq > headSeq && msgsRecv[head].isEmpty()) {
		if (!addResendRequest()) {
		    long currentTS = Util.getCurrentTimestamp();
		    if (startTS == 0) startTS = currentTS;
		    if (currentTS > startTS + 5*Constants.MILLION) {
			startTS = currentTS;
			String log = String.format("Waiting for seq %d while current tail seq is %d", headSeq, tailSeq);
			qLogger.warn(log);
		    }
		}
	    }
	}
	
	RMessage msg = msgsRecv[head];
	head++;
	if (head == qSize) head = 0;
	headSeq = msg.getSeq();
	
	return msg;
    }
}

 
