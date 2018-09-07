package spike.fffc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import spike.fffc.transforms.DataDescriptor;

@RunWith(JUnit4.class)
public class ConfigurationTest {

	@Test
	public void testShouldBuildValidConfugurationWithValidInput() throws IOException {
		String metadataFile = "src/test/resources/metadata.csv";

		List<DataDescriptor> expectedConfig = Arrays.asList(new DataDescriptor("Birth date,10,date"),
				new DataDescriptor("First name,15,string"), new DataDescriptor("Last name,15,string"),
				new DataDescriptor("Weight,5,numeric"));

		List<DataDescriptor> configuration = ConfigurationBuilder.from(metadataFile);

		assertEquals(4, configuration.size());

		assertEquals(expectedConfig, configuration);
	}

	@Test
	public void testThrowsExceptionWithEmptyValueForKey() throws IOException {

		String invalidMetadata = "src/test/resources/invalid-metadata-empty.csv";

		assertThrows(InvalidConfiguration.class, () -> ConfigurationBuilder.from(invalidMetadata));
	}

	@Test
	public void testThrowsExceptionWithBadValueForKey() throws IOException {

		String invalidMetadata = "src/test/resources/invalid-metadata-bad-values.csv";

		assertThrows(InvalidConfiguration.class, () -> ConfigurationBuilder.from(invalidMetadata));
	}

	@Test
	public void testThrowsExceptionWithPartialMetadata() throws IOException {

		String invalidMetadata = "src/test/resources/invalid-metadata-partial.csv";

		assertThrows(InvalidConfiguration.class, () -> ConfigurationBuilder.from(invalidMetadata));
	}

}
