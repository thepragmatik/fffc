package spike.fffc;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface CliOptions extends org.apache.beam.sdk.options.PipelineOptions {

	/**
	 * Get the path to the source data file specified via command line option
	 * <code>--sourcePath</code>
	 * 
	 * @return
	 */
	@Description("Get the path to the source data file")
	@Default.String("src/test/resources/test-data-fffc.txt")
	String getSourcePath();

	void setSourcePath(String sourcePath);

	/**
	 * Get the path to the metadata file specified via command line option
	 * <code>--metadataPath</code>
	 */
	@Description("Get the path to the metadata file ")
	@Default.String("src/test/resources/metadata.csv")
	String getMetadataPath();

	void setMetadataPath(String metadataPath);

	@Description("Get the path to the output file where the transformed data should be written to.")
	@Default.String("target/output.csv")
	String getOutputPath();

	void setOutputPath(String outputPath);
}
