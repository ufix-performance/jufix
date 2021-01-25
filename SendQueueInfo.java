package jufix;

class SendQueueInfo {
    public long dataSeq;
    public byte[] data;
    public int dataStartPos;
    public int dataLength;

    public SendQueueInfo() {
	dataSeq = 0;
    }
}