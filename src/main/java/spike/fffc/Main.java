package spike.fffc;

import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;

import org.apache.beam.repackaged.beam_sdks_java_core.com.google.common.io.CharStreams;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20.com.google.common.collect.Lists;

import spike.fffc.transforms.DataDescriptor;
import spike.fffc.transforms.TransformLineFn;

public class Main {

	public static void main(String[] args) throws IOException {

		PipelineOptions options = PipelineOptionsFactory.create();

		Pipeline pipeline = Pipeline.create(options);

		// TODO: Pass as argument from command line
		String filePath = Paths.get("./src/test/resources/test-data-fffc.txt").toUri().toString();

		String metadataPath = Paths.get("./src/test/resources/metadata.csv").toUri().toString();

		List<DataDescriptor> configuration = ConfigurationBuilder.from(metadataPath);

		PCollection<String> lines = pipeline.apply("FixedFormatFileReader", TextIO.read().from(filePath))
				.setCoder(StringUtf8Coder.of());

		lines.apply(ParDo.of(new TransformLineFn(configuration)));

		pipeline.run().waitUntilFinish();
	}

}

final class ConfigurationBuilder {

	protected static List<DataDescriptor> from(String resourcePath) throws IOException {

		List<String> allLines = Lists.newArrayList();

		ResourceId metadataResource = FileSystems.matchSingleFileSpec(resourcePath).resourceId();

		ReadableByteChannel readableByteChannel = FileSystems.open(metadataResource);

		Reader reader = Channels.newReader(readableByteChannel, StandardCharsets.UTF_8.name());

		allLines.addAll(CharStreams.readLines(reader));

		return buildConfiguration(allLines);
	}

	private static List<DataDescriptor> buildConfiguration(List<String> config) {

		List<DataDescriptor> descriptors = Lists.newArrayList();

		config.forEach((line) -> {

			// FIXME: Remove hard coding!
			descriptors.add(new DataDescriptor(line));

		});

		return descriptors;
	}

}
