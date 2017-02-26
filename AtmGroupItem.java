package ru.bpc.cm.cashmanagement.orm.items;

public class AtmGroupItem {

	private Integer atmGroupId;
	private Integer atmId;
	private String extAtmId;
	private String atmName;
	private String atmGroupName;

	public AtmGroupItem(Integer atmGroupId, Integer atmId, String extAtmId, String atmName, String atmGroupName) {
		super();
		this.atmGroupId = atmGroupId;
		this.atmId = atmId;
		this.extAtmId = extAtmId;
		this.atmName = atmName;
		this.atmGroupName = atmGroupName;
	}

	public Integer getAtmGroupId() {
		return atmGroupId;
	}

	public Integer getAtmId() {
		return atmId;
	}

	public String getExtAtmId() {
		return extAtmId;
	}

	public String getAtmName() {
		return atmName;
	}

	public String getAtmGroupName() {
		return atmGroupName;
	}
}
