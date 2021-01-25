package jufix;

public class RGroupsDictionary {
  
    private int[][] rgDef;
    
    public RGroupsDictionary() {
	rgDef = new int[Constants.MAX_FIX_TAG][];
    }
  
    public void addRG(int[] fields, int len) {
	int tag = fields[0];
	rgDef[tag] = new int[Constants.MAX_FIX_TAG];
        for (int i = 1; i < len; i++) {
	    rgDef[tag][fields[i]] = 1;
	}
    }

    public boolean isNormal(int tag) {
	return (rgDef[tag] == null);
    }

    public boolean isTagInRG(int rgTag, int tag) {
	return (rgDef[rgTag][tag] == 1);
    }
}
