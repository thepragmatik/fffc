package spike.fffc.transforms;

import java.util.List;
import java.util.Set;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
class TransformerTests {

	@SuppressWarnings("deprecation")
	@Test
	void testExtractMetadataConfigKey() throws Exception {

		DoFnTester<String, ConfigKey> extractMetadataConfigKeyFn = DoFnTester.of(new ExtractMetadataFn());

		List<ConfigKey> result = extractMetadataConfigKeyFn.processBundle("Birth date,10,date");

		ConfigKey expected = new ConfigKey("Birth date", 10, "date");

		Assert.assertTrue(result.size() > 0);

		Assert.assertEquals(result.get(0), expected);

	}

}
