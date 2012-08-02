package krasa;

import org.springframework.batch.item.file.transform.FieldExtractor;

/**
 * @author Vojtech Krasa
 */
class StringBuilderFieldExtractor implements FieldExtractor<Item> {
	protected StringBuilder stringBuilder = new StringBuilder();

	@Override
	public Object[] extract(Item item) {
		String s = toString(item);
		return new Object[]{s};
	}

	public String toString(Item item) {
		stringBuilder.append(item.getName());
		stringBuilder.append(",");
		stringBuilder.append(item.getSurname());
		stringBuilder.append(",");
		stringBuilder.append(item.getType());
		stringBuilder.append("\n");
		String s = stringBuilder.toString();
		stringBuilder.setLength(0);
		return s;
	}
}
