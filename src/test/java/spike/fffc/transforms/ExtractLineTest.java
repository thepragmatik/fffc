package spike.fffc.transforms;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import spike.fffc.transforms.TransformLineFn;


@RunWith(JUnitPlatform.class)
class ExtractLineTest {

	
	@Rule
	public TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
	@Test
	void testExtractLineFromSource() {
		
		DoFnTester<String, String> extractlineFn = DoFnTester.of(new TransformLineFn());
		
		
		pipeline.run();
	}

}
