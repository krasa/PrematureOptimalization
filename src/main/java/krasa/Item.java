package krasa;

/**
 * @author Vojtech Krasa
 */
public class Item {
	private String name;
	private String surname;
	private String type;

	public Item(String name, String surname, String type) {
		this.name = name;
		this.surname = surname;
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSurname() {
		return surname;
	}

	public void setSurname(String surname) {
		this.surname = surname;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
}
