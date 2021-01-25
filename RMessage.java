package jufix;

public class RMessage {
    
    private long timestamp;
    
    private byte[] data;
    private int dataStartPos;
    private int dataLength;
    private int msgTypePos;
    private String msgType;
    
    private volatile long seq;
    
    private FieldsParser parser;

    public RMessage() {
	reset();
    }

    public RMessage(byte[] msg, int msgStartPos, int msgLength, int msgTypePos, long msgSeq) {    
	this.data = msg;
	this.dataStartPos = msgStartPos;
	this.dataLength = msgLength;
	setMsgTypePos(msgTypePos);
	this.timestamp = 0;
	this.seq = msgSeq;
    }
  
    private void setMsgTypePos(int pos) {
	msgTypePos = pos;
        pos = dataStartPos + msgTypePos;
	while (data[pos] != Constants.SOH) {
	    pos++;
	}
	msgType = new String(data, dataStartPos + msgTypePos, pos - dataStartPos - msgTypePos);
    }

    public final void copy(RMessage msg) {
	data = msg.getData();
	dataStartPos = msg.getDataStartPos();
	dataLength = msg.getDataLength();
	msgTypePos = msg.getMsgTypePos();
	msgType = msg.getMsgType();
	seq = msg.getSeq();
    }
  
    public final void setGF(long seq) {
	timestamp = -1;
	this.seq = seq;
    }
    
    public final boolean isGF() {
	return (timestamp == -1);
    }
    
    public final boolean isSession() {
	return Util.isSessionMsg(msgType);
    }

    public final void setParser(FieldsParser ps) {
	parser = ps;
    }
    
    public final void setTimestamp(long ts) {
	timestamp = ts;
    }
    
    public final long getTimestamp() {
	return timestamp;
    }
    
    public final void reset() {
	timestamp = 0;
	seq = 0;
    }

    public final byte[] getData() {
	return data;
    }

    public final int getDataStartPos() {
	return dataStartPos;
    }

    public final int getDataLength() {
	return dataLength;
    }
    
    public final String getMessage() {
	return new String(data, dataStartPos, dataLength);
    }

    public final long getSeq() {
	return seq;
    }
  
    public final void setSeq(long seq) {
	this.seq = seq;
    }

    public final boolean isEmpty() {
	return (seq == 0);
    }
  
    public final int getMsgTypePos() {
	return msgTypePos;
    }

    public final String getMsgType() {
	return msgType;
    }
  
    public final int parse() {
	return parser.parse(data, dataStartPos, dataLength);
    }

    public final FieldInfo getField(int tag) {
	return parser.getField(tag);
    }
  
    public final String getString(int tag) {
	FieldInfo info = parser.getField(tag);
	if (info == null) return null;
	return new String(info.data, info.valStartPos, info.valLength);
    }
    
    public final long getLong(int tag) {
	FieldInfo info = parser.getField(tag);
	return Util.convertATOL(info.data, info.valStartPos, info.valLength);
    }

    public final int getInt(int tag) {
        FieldInfo info = parser.getField(tag);
        return Util.convertATOI(info.data, info.valStartPos, info.valLength);
    }

    public final int copyField(int tag, byte[] value, int offset) {
	FieldInfo info = parser.getField(tag);
	if (info == null) return 0;
	System.arraycopy(info.data, info.valStartPos, value, offset, info.valLength);
	return info.valLength;
    }

    public final void getGroups(int tag, RGroups group) {
	group.setParser(parser);
	FieldInfo info = parser.getField(tag);
	group.parse(info.data, info.valStartPos, info.valLength);
    }
}
