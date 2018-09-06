package spike.fffc.transforms;

import java.io.Serializable;

public class DataDescriptor implements Serializable {

	private static final long serialVersionUID = -7801169854022337577L;

	private String columnName;

	private int length;

	private String columnType;

	public DataDescriptor(String source) {
		super();
		String[] fragments = source.split(",");
		// TODO: Review and remove hard coded indices
		this.columnName = fragments[0];
		this.length = Integer.parseInt(fragments[1]);
		this.columnType = fragments[2];
	}

	public String getColumnName() {
		return columnName;
	}

	public int getLength() {
		return length;
	}

	public String getColumnType() {
		return columnType;
	}

	@Override
	public String toString() {
		return "DataDescriptor [columnName=" + columnName + ", length=" + length + ", columnType=" + columnType + "]";
	}

}