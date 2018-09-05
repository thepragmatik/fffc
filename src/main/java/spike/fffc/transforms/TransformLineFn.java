package spike.fffc.transforms;

import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

public class TransformLineFn extends DoFn<String, String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7121367164345947278L;
	
	private PCollectionView<List<String>> metadata;
	
	public TransformLineFn(PCollectionView<List<String>> metadata) {
			this.metadata = metadata;
	}

	@ProcessElement
	public void processLine(ProcessContext ctx, @Element String input) {
		
		System.out.println(input);
		
		System.out.println(ctx.sideInput(this.metadata));
		
	}

}
 