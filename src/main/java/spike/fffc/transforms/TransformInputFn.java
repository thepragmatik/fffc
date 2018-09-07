package spike.fffc.transforms;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.log4j.Logger;

public class TransformInputFn extends DoFn<String, String> {

	private static final String TYPE_DATE = "date";

	private static final String TYPE_STRING = "string";

	private static final long serialVersionUID = 7121367164345947278L;

	private static final Logger LOGGER = Logger.getLogger(TransformInputFn.class);

	private List<DataDescriptor> metadata;

	public TransformInputFn(List<DataDescriptor> metadata) {
		super();
		this.metadata = metadata;
	}

	@ProcessElement
	public void processLine(@Element String input, ProcessContext ctx) {

		validate(input);

		int offset = 0;

		StringBuilder sb = new StringBuilder();

		Iterator<DataDescriptor> itr = metadata.iterator();

		while (itr.hasNext()) {
			DataDescriptor cfg = itr.next();

			LOGGER.debug(String.format("Applying configuration %s to input %s, [offset = %d]", cfg.toString(), input,
					offset));

			String value = input.subSequence(offset, (offset + cfg.getLength())).toString().trim();

			switch (cfg.getColumnType()) {
			case TYPE_STRING:
				if (value.contains(",")) {
					value = "\"" + value + "\"";
				}
				break;
			case TYPE_DATE:
				final DateFormat fromFormat = new SimpleDateFormat("yyyy-mm-dd");
				fromFormat.setLenient(false);
				final DateFormat toFormat = new SimpleDateFormat("dd/mm/yyyy");

				Date parsedDate;
				try {
					parsedDate = fromFormat.parse(value);
				} catch (ParseException e) {
					throw new InvalidDateException("Invalid date format (expected: yyyy-dd-mm). Could not be parsed.",
							e);
				}
				value = toFormat.format(parsedDate);

				break;
			default:
				break;
			}

			sb.append(value);

			offset = offset + cfg.getLength(); // reset offset for next iteration!

			if (itr.hasNext()) {
				sb.append(",");
			}
		}

		LOGGER.debug(sb.toString());

		ctx.output(sb.toString());
	}

	private void validate(String input) throws TruncatedDataException {
		int expectedDataLength = metadata.stream().mapToInt(DataDescriptor::getLength).sum();

		if (input.length() != expectedDataLength) {
			throw new TruncatedDataException(String.format("Invalid data encoutered (expected length = %d, found = %df",
					expectedDataLength, input.length()));
		}

	}

}
