package jufix;

public class Group {
    
    private int fieldsSize;
    private FieldInfo[] fields;
    private FieldsParser parser;
    private int numFields;

    public Group(int size) {
	fieldsSize = size;
	fields = new FieldInfo[fieldsSize];
    }
    
    public final void setParser(FieldsParser fParser) {
	parser = fParser;
    }

    public final void parse(byte[] data, int pos, int dataLen) {
	numFields = parser.parseGroup(data, pos, dataLen, fields);
    }
  
    public final int getNumFields() {
	return numFields;
    }

    public final FieldInfo getField(int index) {
	if (index >= numFields) return null;
	return fields[index];
    }

    public final void getGroups(int index, Object groups) {
        RGroups rGroups =  (RGroups) groups;
        rGroups.setParser(parser);
        FieldInfo info = fields[index];
        rGroups.parse(info.data, info.valStartPos, info.valLength);
    }
}
