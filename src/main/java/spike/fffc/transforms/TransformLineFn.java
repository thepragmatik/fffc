package spike.fffc.transforms;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.log4j.Logger;

public class TransformLineFn extends DoFn<String, String> {

	private static final long serialVersionUID = 7121367164345947278L;

	private static final Logger LOGGER = Logger.getLogger(TransformLineFn.class);

	private List<DataDescriptor> configuration;

	public TransformLineFn(List<DataDescriptor> configuration) {
		super();
		this.configuration = configuration;
	}

	@ProcessElement
	public void processLine(@Element String input, ProcessContext ctx) throws ParseException {
		int offset = 0;

		StringBuilder sb = new StringBuilder();

		Iterator<DataDescriptor> itr = configuration.iterator();

		while (itr.hasNext()) {
			DataDescriptor cfg = itr.next();

			LOGGER.debug(String.format("Applying configuration %s to input %s, [offset = %d]", cfg.toString(), input,
					offset));

			String value = input.subSequence(offset, (offset + cfg.getLength())).toString().trim();

			switch (cfg.getColumnType()) {
			case "string":
				if (value.contains(",")) {
					value = "\"" + value + "\"";
				}
				break;
			case "date":
				final DateFormat fromFormat = new SimpleDateFormat("yyyy-mm-dd");
				final DateFormat toFormat = new SimpleDateFormat("dd/mm/yyyy");

				Date parsedDate = fromFormat.parse(value);
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

}
