package spike.fffc;

import java.io.File;

import org.apache.beam.sdk.transforms.DoFn;

public class ExtractLineFn extends DoFn<File, String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7121367164345947278L;
	
	@ProcessElement
	public void processFile(@Element File input) {
		
	}

}
