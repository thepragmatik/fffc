package spike.fffc;

import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.repackaged.beam_sdks_java_core.com.google.common.io.CharStreams;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import spike.fffc.transforms.DataDescriptor;
import spike.fffc.transforms.TransformInputFn;

public class Main {

	public static void main(String[] args) throws IOException {

		PipelineOptionsFactory.register(CliOptions.class);
		CliOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(CliOptions.class);

		Pipeline pipeline = Pipeline.create(options);

		String metadataPath = Paths.get(options.getMetadataPath()).toUri().toString();

		List<DataDescriptor> configuration = ConfigurationBuilder.from(metadataPath);

		String filePath = Paths.get(options.getSourcePath()).toUri().toString();

		PCollection<String> lines = pipeline.apply("FixedFormatFileReader", TextIO.read().from(filePath))
				.setCoder(StringUtf8Coder.of());

		lines.apply(ParDo.of(new TransformInputFn(configuration))).apply(TextIO.write()
				.withHeader("Birth date,First name,Last name,Weight").to(options.getOutputPath()).withoutSharding());

		pipeline.run().waitUntilFinish();
	}

}

final class ConfigurationBuilder {

	static List<DataDescriptor> from(String resourcePath) throws IOException {

		ResourceId metadataResource = FileSystems.matchSingleFileSpec(resourcePath).resourceId();

		ReadableByteChannel readableByteChannel = FileSystems.open(metadataResource);

		Reader reader = Channels.newReader(readableByteChannel, StandardCharsets.UTF_8.name());

		List<String> allLines = CharStreams.readLines(reader);

		return allLines.stream().map(DataDescriptor::new).collect(Collectors.toList());
	}

}
