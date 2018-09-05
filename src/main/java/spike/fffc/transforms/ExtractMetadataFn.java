package spike.fffc.transforms;

import org.apache.beam.sdk.transforms.DoFn;

public class ExtractMetadataFn extends DoFn<String, ConfigKey> {

	private static final long serialVersionUID = -6913579877338204157L;

	@ProcessElement
	public void getConfigKey(@Element String configRow, ProcessContext ctx) {

//		System.out.println(configRow);

		String[] fragments = configRow.split(",");

		validate(fragments);

		// TODO: Remove hard coding!
		ConfigKey output = new ConfigKey(fragments[0], Integer.parseInt(fragments[1]), fragments[2]);

		ctx.output(output);

	}

	private void validate(String[] fragments) throws IllegalArgumentException {

		if (fragments.length == 0) {
			throw new IllegalArgumentException(
					"Metadata values cannot be null. Please check the metadata input is well defined (String, int, String)");
		}

		// should not be empty
		for (String s : fragments) {
			if (s.trim().isEmpty())
				throw new IllegalArgumentException(
						"Metadata values cannot be null. Please check the metadata input is well defined (String, int, String)");
		}

		Integer.parseInt(fragments[1]); // FIXME: remove hard coding

	}

}