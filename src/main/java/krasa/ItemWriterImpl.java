package krasa;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.WriteFailedException;
import org.springframework.batch.item.WriterNotOpenException;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.util.FileUtils;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;

/**
 * @author Vojtech Krasa
 */
public class ItemWriterImpl<T> implements ItemWriter<T> {


	protected static final Log logger = LogFactory.getLog(JdbcBatchItemWriter.class);

	private static final String DEFAULT_LINE_SEPARATOR = System.getProperty("line.separator");

	private static final String WRITTEN_STATISTICS_NAME = "written";

	private static final String RESTART_DATA_NAME = "current.count";

	private Resource resource;

	private OutputState state = null;

	private LineAggregator<T> lineAggregator;

	private boolean shouldDeleteIfExists = true;

	private String encoding = OutputState.DEFAULT_CHARSET;

	private String lineSeparator = DEFAULT_LINE_SEPARATOR;


	/**
	 * Assert that mandatory properties (lineAggregator) are set.
	 *
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(lineAggregator, "A LineAggregator must be provided.");
	}

	/**
	 * Public setter for the line separator. Defaults to the System property
	 * line.separator.
	 *
	 * @param lineSeparator the line separator to set
	 */
	public void setLineSeparator(String lineSeparator) {
		this.lineSeparator = lineSeparator;
	}

	/**
	 * Public setter for the {@link LineAggregator}. This will be used to
	 * translate the item into a line for output.
	 *
	 * @param lineAggregator the {@link LineAggregator} to set
	 */
	public void setLineAggregator(LineAggregator<T> lineAggregator) {
		this.lineAggregator = lineAggregator;
	}

	/**
	 * Setter for resource. Represents a file that can be written.
	 *
	 * @param resource
	 */
	public void setResource(Resource resource) {
		this.resource = resource;
	}

	/**
	 * Sets encoding for output template.
	 */
	public void setEncoding(String newEncoding) {
		this.encoding = newEncoding;
	}

	/**
	 * Flag to indicate that the target file should be deleted if it already
	 * exists, otherwise it will be appended. If headers are emitted then
	 * appending will cause them to show up in the middle of the file. Defaults
	 * to true (so no appending except on restart).
	 *
	 * @param shouldDeleteIfExists the flag value to set
	 */
	public void setShouldDeleteIfExists(boolean shouldDeleteIfExists) {
		this.shouldDeleteIfExists = shouldDeleteIfExists;
	}

	/**
	 * Writes out a string followed by a "new line", where the format of the new
	 * line separator is determined by the underlying operating system. If the
	 * input is not a String and a converter is available the converter will be
	 * applied and then this method recursively called with the result. If the
	 * input is an array or collection each value will be written to a separate
	 * line (recursively calling this method for each value). If no converter is
	 * supplied the input object's toString method will be used.<br/>
	 *
	 * @param items list of items to be written to output stream
	 * @throws Exception if the transformer or file output fail,
	 *                   WriterNotOpenException if the writer has not been initialized.
	 */
	public void write(T item) throws Exception {
		if (!getOutputState().isInitialized()) {
			throw new WriterNotOpenException("Writer must be open before it can be written to");
		}

		OutputState state = getOutputState();

		String str = lineAggregator.aggregate(item) + lineSeparator;

		try {
			state.write(str);
		} catch (IOException e) {
			throw new WriteFailedException("Could not write data.  The file may be corrupt.", e);
		}
	}

	/**
	 * @see org.springframework.batch.item.ItemStream#close()
	 */
	public void close() {
		if (state != null) {
			state.close();
			state = null;
		}
	}

	/**
	 * Initialize the reader. This method may be called multiple times before
	 * close is called.
	 *
	 * @see org.springframework.batch.item.ItemStream#open(org.springframework.batch.item.ExecutionContext)
	 */
	public void open() throws ItemStreamException {

		Assert.notNull(resource, "The resource must be set");

		if (!getOutputState().isInitialized()) {
			doOpen();
		}
	}

	private void doOpen() throws ItemStreamException {
		OutputState outputState = getOutputState();
		try {
			outputState.initializeBufferedWriter();
		} catch (IOException ioe) {
			throw new ItemStreamException("Failed to initialize writer", ioe);
		}
	}

	/**
	 * @see org.springframework.batch.item.ItemStream#update(ExecutionContext)
	 */
	public void update(ExecutionContext executionContext) {
		if (state == null) {
			throw new ItemStreamException("ItemStream not open or already closed.");
		}

		Assert.notNull(executionContext, "ExecutionContext must not be null");

	}

	// Returns object representing state.
	private OutputState getOutputState() {
		if (state == null) {
			File file;
			try {
				file = resource.getFile();
			} catch (IOException e) {
				throw new ItemStreamException("Could not convert resource to file: [" + resource + "]", e);
			}
			Assert.state(!file.exists() || file.canWrite(), "Resource is not writable: [" + resource + "]");
			state = new OutputState();
			state.setDeleteIfExists(shouldDeleteIfExists);
			state.setEncoding(encoding);
		}
		return (OutputState) state;
	}

	/**
	 * Encapsulates the runtime state of the writer. All state changing
	 * operations on the writer go through this class.
	 */
	private class OutputState {
		// default encoding for writing to output files - set to UTF-8.
		private static final String DEFAULT_CHARSET = "UTF-8";

		private FileOutputStream os;

		// The bufferedWriter over the file channel that is actually written
		Writer outputBufferedWriter;

		// this represents the charset encoding (if any is needed) for the
		// output file
		String encoding = DEFAULT_CHARSET;

		boolean restarted = false;

		boolean shouldDeleteIfExists = true;

		boolean initialized = false;

		/**
		 * @param shouldDeleteIfExists
		 */
		public void setDeleteIfExists(boolean shouldDeleteIfExists) {
			this.shouldDeleteIfExists = shouldDeleteIfExists;
		}

		/**
		 * @param encoding
		 */
		public void setEncoding(String encoding) {
			this.encoding = encoding;
		}

		/**
		 * Close the open resource and reset counters.
		 */
		public void close() {

			initialized = false;
			try {
				if (outputBufferedWriter != null) {
					outputBufferedWriter.close();
				}
			} catch (IOException ioe) {
				throw new ItemStreamException("Unable to close the the ItemWriter", ioe);
			} finally {
				{
					try {
						if (os != null) {
							os.close();
						}
					} catch (IOException ioe) {
						throw new ItemStreamException("Unable to close the the ItemWriter", ioe);
					}
				}
			}
		}

		/**
		 * @param line
		 * @throws IOException
		 */
		public void write(String line) throws IOException {
			if (!initialized) {
				initializeBufferedWriter();
			}

			outputBufferedWriter.write(line);
		}

		/**
		 * Creates the buffered writer for the output file channel based on
		 * configuration information.
		 *
		 * @throws IOException
		 */
		private void initializeBufferedWriter() throws IOException {

			File file = resource.getFile();

			FileUtils.setUpOutputFile(file, restarted, shouldDeleteIfExists);

			os = new FileOutputStream(file.getAbsolutePath(), true);

			outputBufferedWriter = new BufferedWriter(outputBufferedWriter);

			Assert.state(outputBufferedWriter != null);
			initialized = true;
		}

		public boolean isInitialized() {
			return initialized;
		}

	}


}
