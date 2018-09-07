package spike.fffc;

public class InvalidConfiguration extends RuntimeException {

	private static final long serialVersionUID = 5809174660776413364L;

	public InvalidConfiguration(String message) {
		super(message);
	}

	public InvalidConfiguration(String message, Throwable cause) {
		super(message, cause);
	}

}
