package spike.fffc.transforms;

import java.util.List;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitPlatform.class)
class ConverterMetadataTests {

	@SuppressWarnings("deprecation")
	@Test
	void testExtractMetadataConfigKeyWithValidInputs() throws Exception {

		DoFnTester<String, DataDescriptor> extractMetadataConfigKeyFn = DoFnTester.of(new ExtractMetadataFn());

		List<DataDescriptor> result = extractMetadataConfigKeyFn.processBundle("Birth date,10,date");

		DataDescriptor expected = new DataDescriptor("Birth date", 10, "date");

		Assert.assertTrue(result.size() > 0);

		Assert.assertEquals(result.get(0), expected);
	}

	@SuppressWarnings("deprecation")
	@Test
	void testExtractMetadataConfigKeyWithEmptyInput() throws Exception {

		DoFnTester<String, DataDescriptor> extractMetadataConfigKeyFn = DoFnTester.of(new ExtractMetadataFn());

		assertThrows(IllegalArgumentException.class, () -> {
			extractMetadataConfigKeyFn.processBundle("");
		});

	}

	@SuppressWarnings("deprecation")
	@Test
	void testExtractMetadataConfigKeyWithInValidInputs() throws Exception {

		DoFnTester<String, DataDescriptor> extractMetadataConfigKeyFn = DoFnTester.of(new ExtractMetadataFn());

		assertThrows(IllegalArgumentException.class, () -> {
			extractMetadataConfigKeyFn.processBundle("Birth date,ten,date");
		});

	}
}
