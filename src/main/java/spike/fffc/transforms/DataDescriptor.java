package spike.fffc.transforms;

import java.io.Serializable;

import spike.fffc.InvalidConfiguration;

/**
 * Describes a single configuration element.
 *
 */
public class DataDescriptor implements Serializable {

	private static final long serialVersionUID = -7801169854022337577L;

	private static final int INDEX_COLUMN_NAME = 0;

	private static final int INDEX_COLUMN_LENGTH = 1;

	private static final int INDEX_COLUMN_TYPE = 2;

	private String columnName;

	private int length;

	private String columnType;

	public DataDescriptor(String source) {
		validate(source);

		String[] fragments = source.split(",");

		this.columnName = fragments[INDEX_COLUMN_NAME];
		this.length = Integer.parseInt(fragments[INDEX_COLUMN_LENGTH]);
		this.columnType = fragments[INDEX_COLUMN_TYPE];
	}

	/**
	 * Validates that the metadata content is well formed.
	 * 
	 * @param source
	 * @throws InvalidConfiguration if an invalid content is encountered
	 */
	private void validate(String source) throws InvalidConfiguration {
		String[] fragments = source.split(",");
		if (fragments.length != 3) {
			throw new InvalidConfiguration(
					"Invalid format of metadata file. Should have format - Column name, Length, Type ");
		}

		for (String f : fragments) {
			if (f.trim().isEmpty()) {
				throw new InvalidConfiguration(
						"Invalid format of metadata file. Should valid content for the format - Column name, Length, Type ");
			}
		}

		try {
			Integer.parseInt(fragments[INDEX_COLUMN_LENGTH]);
		} catch (NumberFormatException e) {
			throw new InvalidConfiguration("Invalid value in configuration", e);
		}

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