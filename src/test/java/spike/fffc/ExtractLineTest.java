package spike.fffc;

import java.io.File;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;


@RunWith(JUnitPlatform.class)
class ExtractLineTest {

	
	@Rule
	public TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
	@Test
	void testExtractLineFromSource() {
		
		DoFnTester<File, String> extractlineFn = DoFnTester.of(new ExtractLineFn());
		
		
		pipeline.run();
	}

}
