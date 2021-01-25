package jufix;

import java.io.File;

public class Constants {

    public static final String LIB_VERSION = "1.0";

    /* Versions descriptions
       1.0: First testing release
    */
    
    public static final String DIR_DELIM = File.separator;

    public static final String SEQS_PROCESSED = "seqs_p";                                                                    
    public static final String SEQS_SENT = "seqs_s";                                                    
    public static final String SYSTEM_LOG = "session.log"; 
    
    public static final int MILLION = 1000000; 
    
    public static final int LONG_BYTES = Long.SIZE/Byte.SIZE;
    public static final int INT_BYTES = Integer.SIZE/Byte.SIZE;

    public static final int QUEUE_SESSION = 0;

    public static final int SENT_MARK_SIZE = 1 << 20;
    public static final int WINDOW_QUEUE_RR_SIZE = 256;
    
    public static final int SOCKET_RECV_WINDOWS = 1 << 16;    
    public static final int SOCKET_RECV_BUF_SIZE = 1 << 24;
    public static final int SOCKET_SEND_BUF_SIZE = 1 << 24;

    public static final int CMD_BLOCK_SIZE = 32;
    
    public static final double TR_THRESHOLD = 1.2;
    public static final double DISCONNECT_THRESHOLD  = 2.4;

    public static final int MAX_FIX_TAG = 1 << 15;
    public static final char SOH  = (char) 1;
    public static final char EQUAL = '=';
    public static final String  SEQ_PATTERN = "34=";
    public static final int SEQ_PATTERN_LENGTH = 3;
    public static final String SENDING_TIME_PATTERN = "52=";
    public static final int SENDING_TIME_PATTERN_LENGTH = 3;

    public static final String MSG_TYPE_HEARTBEAT = "0";
    public static final String MSG_TYPE_TEST_REQUEST = "1";
    public static final String MSG_TYPE_RESEND_REQUEST = "2";
    public static final String MSG_TYPE_REJECT = "3";
    public static final String MSG_TYPE_SEQUENCE_RESET = "4";
    public static final String MSG_TYPE_LOGOUT = "5";
    public static final String MSG_TYPE_LOGON = "A";
    public static final String MSG_TYPE_XML_NON_FIX = "n";
    public static final String MSG_TYPE_GF = "GF";

    public static final int CHECKSUM_TAG_PART_SIZE = 3;
    public static final int CHECKSUM_VALUE_PART_SIZE = 4;
    public static final int CHECKSUM_SIZE = (CHECKSUM_TAG_PART_SIZE + CHECKSUM_VALUE_PART_SIZE);

    public static final int SENDING_TIME_SIZE  = 21;
    public static final int DISTANCE_SENDING_ORIG_SENDING = (SENDING_TIME_SIZE + 5);

    public static final int BEGIN_STRING = 8;
    public static final int BODY_LENGTH = 9;
    public static final int MSG_TYPE = 35;
    public static final int SENDER_COMP_ID = 49;
    public static final int TARGET_COMP_ID = 56;
    public static final int MSG_SEQ_NUM = 34;
    public static final int SENDING_TIME = 52;
    public static final int ORIG_SENDING_TIME = 122;
    public static final int POS_DUP_FLAG = 43;
    public static final int ENCRYPT_METHOD = 98;
    public static final int HEART_BT_IN = 108;
    public static final int DEFAULT_APPL_VER_ID = 1137;
    public static final int CHECKSUM = 10;
    public static final int BEGIN_SEQ_NO = 7;
    public static final int END_SEQ_NO = 16;
    public static final int ENCRYPT_METHOD_NONE = 0;
    public static final int TEST_REQ_ID = 112;
    public static final int GAP_FILL_FLAG = 123;
    public static final int NEW_SEQ_NO = 36;
}
