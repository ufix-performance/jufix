package jufix;

class SentMark {
    
    private int markSize;
    private SeqSentInfo[] marks;
    
    public SentMark(int size) {
	markSize = size;
	marks = new SeqSentInfo[size];
	for (int i = 0; i < size; i++) {
	    marks[i] = new SeqSentInfo();
	}
    }

    public SeqSentInfo getMark(long seq) {
	int index = Util.getQueueIndex(seq, markSize);
	SeqSentInfo info = marks[index];
	if (info.msgSeq == seq) return info;
	return null;
    }

    public SeqSentInfo getLastMark(long seq) {
	while(seq > 0) {
	    SeqSentInfo info = getMark(--seq);
	    if (info != null) return info;
	}
	return null;
    }

    public void setMark(int qIndex, long msgSeq, long dataSeq) {
	int index = Util.getQueueIndex(msgSeq, markSize);
	SeqSentInfo info = marks[index];
	info.qIndex = qIndex;
	info.msgSeq = msgSeq;
	info.dataSeq = dataSeq;
    }
}
