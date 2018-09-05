/**
 * 
 */
package spike.fffc.transforms;

import org.apache.beam.sdk.transforms.DoFn;

/**
 * Responsible for extracting date from the input
 */
public class ExtractDateFn extends DoFn<String, String>{

	
	@ProcessElement
	public void extractDate(@Element String line) {
		
		
		
		
	}
	
	
}
