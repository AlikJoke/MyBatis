package ru.bpc.cm.cashmanagement.orm.items;

public class CodesItem {

	private int mainCurrCode;
	private int secCurrCode;
	private int sec2CurrCode;
	private int sec3CurrCode;

	public CodesItem() {
	}

	public CodesItem(int mainCurrCode, int secCurrCode, int sec2CurrCode, int sec3CurrCode) {
		super();
		this.mainCurrCode = mainCurrCode;
		this.secCurrCode = secCurrCode;
		this.sec2CurrCode = sec2CurrCode;
		this.sec3CurrCode = sec3CurrCode;
	}

	public int getMainCurrCode() {
		return mainCurrCode;
	}

	public void setMainCurrCode(int mainCurrCode) {
		this.mainCurrCode = mainCurrCode;
	}

	public int getSecCurrCode() {
		return secCurrCode;
	}

	public void setSecCurrCode(int secCurrCode) {
		this.secCurrCode = secCurrCode;
	}

	public int getSec2CurrCode() {
		return sec2CurrCode;
	}

	public void setSec2CurrCode(int sec2CurrCode) {
		this.sec2CurrCode = sec2CurrCode;
	}

	public int getSec3CurrCode() {
		return sec3CurrCode;
	}

	public void setSec3CurrCode(int sec3CurrCode) {
		this.sec3CurrCode = sec3CurrCode;
	}
}
