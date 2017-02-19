package ru.bpc.cm.cashmanagement.orm.items;

public class BalanceItem {

	private String atmId;
	private String cassType;
	private String cassNumber;
	private String remainingLoad;
	private String remainingCalc;

	public BalanceItem(String atmId, String cassType, String cassNumber, String remainingLoad, String remainingCalc) {
		super();
		this.atmId = atmId;
		this.cassType = cassType;
		this.cassNumber = cassNumber;
		this.remainingLoad = remainingLoad;
		this.remainingCalc = remainingCalc;
	}

	public String getAtmId() {
		return atmId;
	}

	public void setAtmId(String atmId) {
		this.atmId = atmId;
	}

	public String getCassType() {
		return cassType;
	}

	public void setCassType(String cassType) {
		this.cassType = cassType;
	}

	public String getCassNumber() {
		return cassNumber;
	}

	public void setCassNumber(String cassNumber) {
		this.cassNumber = cassNumber;
	}

	public String getRemainingLoad() {
		return remainingLoad;
	}

	public void setRemainingLoad(String remainingLoad) {
		this.remainingLoad = remainingLoad;
	}

	public String getRemainingCalc() {
		return remainingCalc;
	}

	public void setRemainingCalc(String remainingCalc) {
		this.remainingCalc = remainingCalc;
	}
}
