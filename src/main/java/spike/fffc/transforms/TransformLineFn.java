package spike.fffc.transforms;

import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

public class TransformLineFn extends DoFn<String, String> {

	private static final long serialVersionUID = 7121367164345947278L;

	private PCollectionView<List<DataDescriptor>> metadata;

	public TransformLineFn(PCollectionView<List<DataDescriptor>> metadata) {
		this.metadata = metadata;
	}

	@ProcessElement
	public void processLine(ProcessContext ctx, @Element String input) {

		List<DataDescriptor> sideInput = ctx.sideInput(this.metadata);

		sideInput.forEach((d) -> {
//			String columnName = d.getColumnName();
			int length = d.getLength();
//			String columnType = d.getColumnType();

			System.out.println(input);

			int offset = 0; // TODO: Discover this while processing metadata
			String val = input.subSequence(offset, (offset + length)).toString();
			System.out.println(val.trim());
		});

	}

}
