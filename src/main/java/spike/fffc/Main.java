package spike.fffc;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import spike.fffc.transforms.TransformLineFn;

/**
 * 
 */
public class Main {

	public static void main(String[] args) throws IOException {

		PipelineOptions options = PipelineOptionsFactory.create();

		Pipeline pipeline = Pipeline.create(options);

		// TODO: Pass as argument from command line
		String filePath = Paths.get("./src/test/resources/test-data-fffc.txt").toUri().toString();

		String metadataPath = Paths.get("./src/test/resources/metadata.csv").toUri().toString();

		PCollectionView<List<String>> metadata = pipeline.apply("LoadMetadata", TextIO.read().from(metadataPath))
				.apply(View.<String>asList());

		PCollection<String> lines = pipeline.apply("FixedFormatFileReader", TextIO.read().from(filePath))
				.setCoder(StringUtf8Coder.of());

		lines.apply(ParDo.of(new TransformLineFn(metadata)).withSideInputs(metadata));

		// start the processing pipeline
		pipeline.run().waitUntilFinish();

	}

}
