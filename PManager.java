package jufix;

import java.io.File;

class PManager {

    private String dir;
    private String qDir;
    
    private FileHandler seqsProcessed;
    private FileHandler seqsSent;
    
    private FileHandler[] dataQueue;
    private FileHandler systemLog;


    public PManager(String pDir, String sDir, int numSendQueues) {
	
	dir = String.format("%s%s%s", pDir, Constants.DIR_DELIM, sDir);
	Util.dirCreate(dir);
    
	qDir = String.format("%s%s%s", dir, Constants.DIR_DELIM, "queue");                 
	Util.dirCreate(qDir);

	seqsProcessed = new FileHandler(String.format("%s%s%s", dir, Constants.DIR_DELIM, Constants.SEQS_PROCESSED), 'a', true);
	seqsSent = new FileHandler(String.format("%s%s%s", dir, Constants.DIR_DELIM, Constants.SEQS_SENT), 'a', true);
	systemLog = new FileHandler(String.format("%s%s%s", dir, Constants.DIR_DELIM, Constants.SYSTEM_LOG), 'a', false);
	
	dataQueue = new FileHandler[numSendQueues];
    }
  
    public FileHandler getSystemLog() {
	return systemLog;
    }

    public FileHandler initDataQueue(int qIndex) {
	String fileName = String.format("%s%s%d", qDir, Constants.DIR_DELIM, qIndex);
	dataQueue[qIndex] = new FileHandler(fileName, 'a', true);
	return dataQueue[qIndex];
    }
  
    public FileHandler getDataRecoverFile(int qIndex) {
	String fileName = String.format("%s%s%d", qDir, Constants.DIR_DELIM, qIndex);
	return new FileHandler(fileName, 'r', true);
    }
  
    public void close(FileHandler fileHandler) {
	if (fileHandler != null) {
	    fileHandler.close();
	}
    }

    public void writeSeqProcessed(long seq) {
	byte[] bytes = new byte[8];
	Util.convertToBytes(seq, bytes, 0);
	seqsProcessed.write(bytes, 0, 8);
	seqsProcessed.flush();
    }

    public void writeSeqSent(int qIndex, long seq, long dataSeq) {
	byte[] buf = new byte[Constants.INT_BYTES + 2*Constants.LONG_BYTES];
	Util.convertToBytes(qIndex, buf, 0);
	Util.convertToBytes(seq, buf, Constants.INT_BYTES);
	Util.convertToBytes(dataSeq, buf, Constants.INT_BYTES + Constants.LONG_BYTES);
	seqsSent.write(buf, 0, Constants.INT_BYTES + 2*Constants.LONG_BYTES);
	seqsSent.flush();
    }

    public long readLastSeqProcessed() {
	String fileName = String.format("%s%s%s", dir, Constants.DIR_DELIM, Constants.SEQS_PROCESSED);
	FileHandler fileSeqsProcessed = new FileHandler(fileName, 'r', true);
	
	long size = fileSeqsProcessed.getFileSize();
	if (size == 0) return 0;
	fileSeqsProcessed.seek(size - Constants.LONG_BYTES);
	
	byte[] buf = new byte[Constants.LONG_BYTES];
	fileSeqsProcessed.read(buf, 0, Constants.LONG_BYTES);
	fileSeqsProcessed.close();
	return Util.convertToLong(buf, 0);
    }

    public long readSentMarks(SentMark marks, long[] lastSeqs) {
	String fileName = String.format("%s%s%s", dir, Constants.DIR_DELIM, Constants.SEQS_SENT); 
	
	FileHandler fileSeqsSent = new FileHandler(fileName, 'r', true);
	
	byte[] buf = new byte[1 << 17];
	int maxReadSize = (Constants.INT_BYTES + 2*Constants.LONG_BYTES)*(1 << 12);
	
	long lastSeq = 0;
	int lastQIndex = 0;
	
	while (true) {
	    int len = fileSeqsSent.read(buf, 0, maxReadSize);
	    int pos = 0;
	    while (true) {
		if (pos >= len) break;
		int qIndex = Util.convertToInt(buf, pos);
		pos += Constants.INT_BYTES;
		long msgSeq = Util.convertToLong(buf, pos);
		pos += Constants.LONG_BYTES;
		long dataSeq = Util.convertToLong(buf, pos);
		pos += Constants.LONG_BYTES;
		
		marks.setMark(qIndex, msgSeq, dataSeq);
		if (qIndex != lastQIndex) {
		    lastSeqs[lastQIndex] += (msgSeq - 1 - lastSeq);
		    lastQIndex = qIndex;
		}
		lastSeqs[qIndex] = dataSeq;
		lastSeq = msgSeq;
	    }
	    if (len != maxReadSize) break;
	}
	return lastSeq;
    }
}
