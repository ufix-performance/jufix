package jufix;

public class SessionOption {
    
    private static final int MAX_HEADER_OPTIONS = 64;
    
    public static final int DEFAULT_HB_TIME = 30;

    public String sessionID;

    public String fixVersion;
    
    public RGroupsDictionary rgDictionary;

    public int maxBodyLengthSize;
    public int maxMsgSeqSize;

    public String senderCompID;

    public String targetCompID;

    public int heartBTIn;

    public int defaultApplVerID;
    
    public int recvQueueSize;

    public int numSendQueues;
    public int[] sendQueuesSize;
    public int[] sendQueuesReserve;
    public int[]  sendQueuesFlag;

    public String pDir;

    public int windowMarkSize;

    public int fixMsgAvgLength;
    public int fixMsgMaxLength;

    public String[] headerOptionsVal;
    public int[] headerOptionsTag;
    public int numHeaderOptions;
    
    public String[] logonOptionsVal;
    public int[] logonOptionsTag;
    public int numLogonOptions;

    public SessionOption() {
	sessionID = null;
	rgDictionary = null;
	maxBodyLengthSize = 4;
	maxMsgSeqSize = 10;
	heartBTIn = 30;
	defaultApplVerID = -1;
	recvQueueSize = 1 << 20; 
	
	numSendQueues = 1;
	sendQueuesSize = null;
	sendQueuesReserve = null;
	sendQueuesFlag = null;
	defaultApplVerID = -1;
	pDir = "data";
	
	windowMarkSize = 32; 
	fixMsgAvgLength = 256;
	fixMsgMaxLength = 1 << 20;

	numHeaderOptions = 0;
	numLogonOptions = 0;
    }
    
    public void addRepeatingGroup(int[] tags, int num_tags) {
	if (rgDictionary == null) rgDictionary = new RGroupsDictionary();
	rgDictionary.addRG(tags, num_tags);
    }
    
    public void addHeaderOption(int tag, String val) {
	if (headerOptionsVal == null) {
	    headerOptionsTag = new int[MAX_HEADER_OPTIONS];
	    headerOptionsVal = new String[MAX_HEADER_OPTIONS];
	}
	
	headerOptionsTag[numHeaderOptions] = tag;
	headerOptionsVal[numHeaderOptions] = val;
	numHeaderOptions++;
    }

    
    public void addLogonOption(int tag, String val) {
        if (logonOptionsVal == null) {
            logonOptionsTag = new int[MAX_HEADER_OPTIONS];
            logonOptionsVal = new String[MAX_HEADER_OPTIONS];
        }

        logonOptionsTag[numLogonOptions] = tag;
        logonOptionsVal[numLogonOptions] = val;
        numLogonOptions++;
    }
}
