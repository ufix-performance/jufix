package jufix;

public class RGroups {

    private int maxNumGroups;
    private FieldInfo[] fields;
    private int numGroups;
    private FieldsParser parser;
    
    public RGroups(int size) {
	maxNumGroups = size;
	fields = new FieldInfo[maxNumGroups];
	numGroups = 0;
    }
  
    public void setParser(FieldsParser parser) {
	this.parser = parser;
    }

    public void parse(byte[] data, int dataPos, int dataLength) {
	numGroups = parser.parseGroups(data, dataPos, dataLength, fields); 
    }
    
    public int getNumGroups() {
	return numGroups;
    }
    
    public void getGroup(int index, Group group) {
	group.setParser(parser);
	group.parse(fields[index].data, fields[index].valStartPos, fields[index].valLength);
    }
}
