package spike.fffc.transforms;

public class TruncatedDataException extends RuntimeException {

	private static final long serialVersionUID = 6294594830570041310L;

	public TruncatedDataException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public TruncatedDataException(String msg) {
		super(msg);
	}

}
