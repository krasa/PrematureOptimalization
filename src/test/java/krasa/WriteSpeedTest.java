package krasa;

import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Arrays;

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

}
