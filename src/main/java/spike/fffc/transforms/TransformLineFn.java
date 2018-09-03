package spike.fffc.transforms;

import org.apache.beam.sdk.transforms.DoFn;

public class TransformLineFn extends DoFn<String, String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7121367164345947278L;
	
	@ProcessElement
	public void processLine(@Element String input) {
		
		System.out.println(input);
		
	}

}
