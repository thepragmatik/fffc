package spike.fffc.transforms;

import java.util.Iterator;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;

public class TransformLineFn extends DoFn<String, String> {

	private static final long serialVersionUID = 7121367164345947278L;

	private List<DataDescriptor> configuration;

	public TransformLineFn(List<DataDescriptor> configuration) {
		super();
		this.configuration = configuration;
	}

	@ProcessElement
	public void processLine(@Element String input, ProcessContext ctx) {
//		System.out.println(input);

		int offset = 0;

		StringBuilder sb = new StringBuilder();

		Iterator<DataDescriptor> itr = configuration.iterator();

		DataDescriptor cfg = null;

		while (itr.hasNext()) {
			cfg = itr.next();

			System.out.println(String.format("Applying configuration %s to input %s, [offset = %d]", cfg.toString(),
					input, offset));

			sb.append(input.subSequence(offset, (offset + cfg.getLength())).toString().trim());

			offset = offset + cfg.getLength(); // reset offset for next iteration!

			if (itr.hasNext()) {
				sb.append(",");
			}
		}

		System.out.println(sb.toString());

		ctx.output(sb.toString());
	}

}
