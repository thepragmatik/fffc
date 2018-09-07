package spike.fffc.transforms;

import java.util.List;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableList;

@RunWith(JUnit4.class)
public class TransformerTest {

	@Rule
	public final transient TestPipeline pipeline = TestPipeline.create();

	@Rule
	public transient ExpectedException thrown = ExpectedException.none();

	@Test
	@Category(NeedsRunner.class)
	public void testTransformationWithValidData() {

		PCollection<String> input = pipeline.apply(Create.of(ImmutableList.of(
				"1970-01-01John           Smith           81.5", "1975-01-31Jane           Doe             61.1",
				"1988-11-28Bob            Big            102.4", "1977-01-01John           Ab,ram          90.3"))
				.withCoder(StringUtf8Coder.of()));

		List<DataDescriptor> configuration = createTestMetadata();

		PCollection<String> transformed = input.apply(ParDo.of(new TransformInputFn(configuration)));

		PAssert.that(transformed).containsInAnyOrder("01/01/1970,John,Smith,81.5", "31/01/1975,Jane,Doe,61.1",
				"01/01/1977,John,\"Ab,ram\",90.3", "28/11/1988,Bob,Big,102.4");

		pipeline.run().waitUntilFinish();
	}

	@Test
	@Category(NeedsRunner.class)
	public void testTransformationWithInvalidDateFormatThrowsException() {
		PCollection<String> input = pipeline.apply(Create
				.of(ImmutableList.of("28-11-1988Bob            Big            102.4")).withCoder(StringUtf8Coder.of()));

		List<DataDescriptor> configuration = createTestMetadata();

		input.apply(ParDo.of(new TransformInputFn(configuration)));

		thrown.expectCause(Is.is(InvalidDateException.class));

		pipeline.run().waitUntilFinish();

	}

	@Test
	@Category(NeedsRunner.class)
	public void testTransformationWithTruncatedDataThrowsException() {
		PCollection<String> input = pipeline.apply(
				Create.of(ImmutableList.of("28-11-1988Bob            Big           ")).withCoder(StringUtf8Coder.of()));

		List<DataDescriptor> configuration = createTestMetadata();

		input.apply(ParDo.of(new TransformInputFn(configuration)));

		thrown.expectCause(Is.is(TruncatedDataException.class));

		pipeline.run().waitUntilFinish();

	}

	private List<DataDescriptor> createTestMetadata() {
		return ImmutableList.of(new DataDescriptor("Birth date,10,date"), new DataDescriptor("First name,15,string"),
				new DataDescriptor("Last name,15,string"), new DataDescriptor("Weight,5,numeric"));
	}

}
