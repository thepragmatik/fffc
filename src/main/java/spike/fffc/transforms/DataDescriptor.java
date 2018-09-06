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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
		result = prime * result + ((columnType == null) ? 0 : columnType.hashCode());
		result = prime * result + length;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataDescriptor other = (DataDescriptor) obj;
		if (columnName == null) {
			if (other.columnName != null)
				return false;
		} else if (!columnName.equals(other.columnName))
			return false;
		if (columnType == null) {
			if (other.columnType != null)
				return false;
		} else if (!columnType.equals(other.columnType))
			return false;
		if (length != other.length)
			return false;
		return true;
	}

}