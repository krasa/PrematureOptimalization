package krasa;

/**
 * @author Vojtech Krasa
 */
public interface ItemWriter<T> {
	void write(T item) throws Exception;
}
