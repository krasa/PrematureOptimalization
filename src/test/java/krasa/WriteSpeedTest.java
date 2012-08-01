package krasa;

import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.FileSystemResource;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * @author Vojtech Krasa
 */
public class WriteSpeedTest {
	@Test
	public void testName() throws Exception {
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
	public void testName2() throws Exception {
//		FileOutputStream os = new FileOutputStream("/tmp/a.txt", true);
//		FileChannel fileChannel = os.getChannel();
//		Writer bw = Channels.newWriter(fileChannel, "UTF-8");
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

	@Test
	public void testName3() throws Exception {
		int i1 = 100000;
		long time = measureSpringWriter(i1);
		long time2 = measureSpringWriter2(i1);
		long time3 = measureMyWriter(i1);
		System.out.println(time + " Spring writer single item total time ");
		System.out.println(time2 + " Spring writer total time ");
		System.out.println(time3 + " My writer total time     ");

	}

	private long measureSpringWriter2(int i1) throws Exception {
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

	private long measureMyWriter(int i1) throws Exception {
		ItemWriterImpl<Item> itemItemWriter = createMyWriter();

		long start = System.nanoTime();
		for (int i = 0; i < i1; i++) {
			itemItemWriter.write(new Item(String.valueOf(i), "kuk", "foo"));
		}
		itemItemWriter.close();

		return System.nanoTime() - start;
	}

	private long measureSpringWriter(int i1) throws Exception {
		FlatFileItemWriter<Item> itemItemWriter = createSpringWriter();


		long start = System.nanoTime();
		for (int i = 0; i < i1; i++) {
			itemItemWriter.write(Collections.singletonList(new Item(String.valueOf(i), "kuk", "foo")));
		}
		itemItemWriter.close();

		return System.nanoTime() - start;
	}

	private FlatFileItemWriter<Item> createSpringWriter() {
		FlatFileItemWriter<Item> itemItemWriter = new FlatFileItemWriter<Item>();
		DelimitedLineAggregator<Item> lineAggregator = new DelimitedLineAggregator<Item>();
		lineAggregator.setDelimiter(",");
		BeanWrapperFieldExtractor<Item> fieldExtractor = new BeanWrapperFieldExtractor<Item>();
		fieldExtractor.setNames(new String[]{"name", "surname", "type"});
		lineAggregator.setFieldExtractor(fieldExtractor);
		itemItemWriter.setLineAggregator(lineAggregator);
		itemItemWriter.setResource(new FileSystemResource("/tmp/a1.txt"));
		itemItemWriter.open(new ExecutionContext());
		return itemItemWriter;
	}

	private ItemWriterImpl<Item> createMyWriter() {
		ItemWriterImpl<Item> myitemItemWriter = new ItemWriterImpl<Item>();
		DelimitedLineAggregator<Item> lineAggregator = new DelimitedLineAggregator<Item>();
		lineAggregator.setDelimiter(",");
		BeanWrapperFieldExtractor<Item> fieldExtractor = new BeanWrapperFieldExtractor<Item>();
		fieldExtractor.setNames(new String[]{"name", "surname", "type"});
		lineAggregator.setFieldExtractor(fieldExtractor);
		myitemItemWriter.setLineAggregator(lineAggregator);
		myitemItemWriter.setResource(new FileSystemResource("/tmp/a.txt"));
		myitemItemWriter.open();
		return myitemItemWriter;
	}

}
