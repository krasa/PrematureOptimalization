package krasa;

import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.core.io.FileSystemResource;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * @author Vojtech Krasa
 */
public class WriteSpeedTest extends SimpleBenchmark {
	@Test
	public void writeSpeed() throws Exception {
		char[] chars = new char[100 * 1024 * 1024];
		Arrays.fill(chars, 'A');
		String text = new String(chars);
		long start = System.nanoTime();
		BufferedWriter bw = new BufferedWriter(new FileWriter("/tmp/a.txt"));
		bw.write(text);
		bw.close();
		long time = System.nanoTime() - start;
		System.out.println("Wrote " + chars.length * 1000L / time + " MB/s.");

	}

	@Test
	public void writeSpeedManyParts() throws Exception {
//		Writer bw = createFileChannelWriter();
		BufferedWriter bw = new BufferedWriter(new FileWriter("/tmp/a.txt"));
		long start = System.nanoTime();

		for (int i = 0; i < 1024 * 1024; i++) {
			char[] chars = new char[100];
			Arrays.fill(chars, 'A');
			String text = new String(chars);
			bw.write(text);
		}

		bw.close();
		long time = System.nanoTime() - start;
		System.out.println("Wrote " + 100 * 1024 * 1024 * 1000L / time + " MB/s.");

	}

	private Writer createFileChannelWriter() throws FileNotFoundException {
		FileOutputStream os = new FileOutputStream("/tmp/a.txt", true);
		FileChannel fileChannel = os.getChannel();
		return Channels.newWriter(fileChannel, "UTF-8");
	}


	public static void main(String[] args) throws Exception {
		Runner.main(WriteSpeedTest.class, args);
	}

	public void timeMyOperation(int reps) {
		for (int i = 0; i < reps; i++) {
			int numberOfItemsToWrite = 1000;
			try {
				naiveWriter(numberOfItemsToWrite, new StringBuilderFieldExtractor());
//				measureSpringWriterWritingByOne(numberOfItemsToWrite);
//				measureSpringWriterChunks(numberOfItemsToWrite);
//				measureMyItemWriter(numberOfItemsToWrite, createBeanWrapperFieldExtractor());
//				measureMyItemWriter(numberOfItemsToWrite, new StringBuilderFieldExtractor());
//				measureMyWriter(numberOfItemsToWrite, new MyFieldExtractor());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private long naiveWriter(int i1, StringBuilderFieldExtractor myFieldExtractor) throws IOException {
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("/tmp/naive.txt"));

		long start = System.nanoTime();
		for (int i = 0; i < i1; i++) {
			Item item = new Item(String.valueOf(i), "kuk", "foo");
			bufferedWriter.write(myFieldExtractor.toString(item));
		}
		bufferedWriter.close();

		return System.nanoTime() - start;
	}

	private long measureSpringWriterChunks(int i1) throws Exception {
		FlatFileItemWriter<Item> itemItemWriter = createSpringWriter();


		long start = System.nanoTime();
		int batchSize = 5;
		ArrayList<Item> items = new ArrayList<Item>(batchSize);
		for (int processed = 0; processed < i1; ) {
			for (int j = 0; j < batchSize; j++) {
				items.add(new Item(String.valueOf(j), "kuk", "foo"));
			}
			itemItemWriter.write(items);
			items.clear();
			processed = processed + batchSize;
		}

		itemItemWriter.close();

		return System.nanoTime() - start;

	}

	private long measureSpringWriterWritingByOne(int i1) throws Exception {
		FlatFileItemWriter<Item> itemItemWriter = createSpringWriter();


		long start = System.nanoTime();
		for (int i = 0; i < i1; i++) {
			itemItemWriter.write(Collections.singletonList(new Item(String.valueOf(i), "kuk", "foo")));
		}
		itemItemWriter.close();

		return System.nanoTime() - start;
	}

	private long measureMyItemWriter(int i1, FieldExtractor fieldExtractor) throws Exception {
		ItemWriterImpl<Item> itemItemWriter = createMyWriter(fieldExtractor);

		long start = System.nanoTime();
		for (int i = 0; i < i1; i++) {
			itemItemWriter.write(new Item(String.valueOf(i), "kuk", "foo"));
		}
		itemItemWriter.close();

		return System.nanoTime() - start;
	}

	private FlatFileItemWriter<Item> createSpringWriter() {
		FlatFileItemWriter<Item> itemItemWriter = new FlatFileItemWriter<Item>();
		DelimitedLineAggregator<Item> lineAggregator = new DelimitedLineAggregator<Item>();
		lineAggregator.setDelimiter(",");
		lineAggregator.setFieldExtractor(createBeanWrapperFieldExtractor());
		itemItemWriter.setLineAggregator(lineAggregator);
		itemItemWriter.setResource(new FileSystemResource("/tmp/a1.txt"));
		itemItemWriter.open(new ExecutionContext());
		return itemItemWriter;
	}

	private FieldExtractor<Item> createBeanWrapperFieldExtractor() {
		BeanWrapperFieldExtractor<Item> fieldExtractor = new BeanWrapperFieldExtractor<Item>();
		fieldExtractor.setNames(new String[]{"name", "surname", "type"});
		return fieldExtractor;
	}

	private ItemWriterImpl<Item> createMyWriter(FieldExtractor fieldExtractor) {
		ItemWriterImpl<Item> myitemItemWriter = new ItemWriterImpl<Item>();
		DelimitedLineAggregator<Item> lineAggregator = new DelimitedLineAggregator<Item>();
		lineAggregator.setDelimiter(",");
		lineAggregator.setFieldExtractor(fieldExtractor);
		myitemItemWriter.setLineAggregator(lineAggregator);
		myitemItemWriter.setResource(new FileSystemResource("/tmp/a.txt"));
		myitemItemWriter.open();
		return myitemItemWriter;
	}

}
