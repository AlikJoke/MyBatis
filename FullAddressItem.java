package ru.bpc.cm.cashmanagement.orm.items;

public class FullAddressItem {

	private String state;
	private String city;
	private String street;
	private String name;

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}
	
	public String getName() {
		return name;
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public FullAddressItem(String state, String city, String street) {
		super();
		this.state = state;
		this.city = city;
		this.street = street;
	}

	public FullAddressItem(String state, String city, String street, String name) {
		super();
		this.state = state;
		this.city = city;
		this.street = street;
		this.name = name;
	}

}
