package spike.fffc;

import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;

import org.apache.beam.repackaged.beam_sdks_java_core.com.google.common.io.CharStreams;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v20.com.google.common.collect.Lists;

import spike.fffc.transforms.DataDescriptor;
import spike.fffc.transforms.ExtractMetadataFn;
import spike.fffc.transforms.TransformLineFn;

public class Main {

	public static void main(String[] args) throws IOException {

		PipelineOptions options = PipelineOptionsFactory.create();

		Pipeline pipeline = Pipeline.create(options);

		// TODO: Pass as argument from command line
		String filePath = Paths.get("./src/test/resources/test-data-fffc.txt").toUri().toString();

		String metadataPath = Paths.get("./src/test/resources/metadata.csv").toUri().toString();

		PCollection<String> metadataContent = pipeline.apply("LoadMetadata", TextIO.read().from(metadataPath));

		PCollectionView<List<DataDescriptor>> metadata = metadataContent.apply(Window.<String>into(new GlobalWindows()))
				.apply(ParDo.of(new ExtractMetadataFn())).apply(View.asList());

		// Build Configuration here!  
		List<String> allLines = Lists.newArrayList();

		Reader reader = Channels.newReader(FileSystems.open(FileSystems.matchSingleFileSpec(metadataPath).resourceId()),
				StandardCharsets.UTF_8.name());

		allLines.addAll(CharStreams.readLines(reader));

		List<DataDescriptor> configuration = buildConfiguration(allLines);

		System.out.println(configuration);

		PCollection<String> lines = pipeline.apply("FixedFormatFileReader", TextIO.read().from(filePath))
				.setCoder(StringUtf8Coder.of());

		lines.apply(ParDo.of(new TransformLineFn(configuration)).withSideInputs(metadata));

		// start the processing pipeline
		pipeline.run().waitUntilFinish();

	}

	private static List<DataDescriptor> buildConfiguration(List<String> config) {

		List<DataDescriptor> descriptors = Lists.newArrayList();

		config.forEach((line) -> {

			String[] fragments = line.split(",");

			// FIXME: Remove hard coding!
			descriptors.add(new DataDescriptor(fragments[0], Integer.parseInt(fragments[1]), fragments[2]));

		});

		return descriptors;

	}

}
