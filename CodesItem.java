package ru.bpc.cm.cashmanagement.orm.items;

public class CodesItem {

	private Integer mainCurrCode;
	private Integer secCurrCode;
	private Integer sec2CurrCode;
	private Integer sec3CurrCode;

	public CodesItem() {
	}

	public CodesItem(Integer mainCurrCode, Integer secCurrCode, Integer sec2CurrCode, Integer sec3CurrCode) {
		super();
		this.mainCurrCode = mainCurrCode;
		this.secCurrCode = secCurrCode;
		this.sec2CurrCode = sec2CurrCode;
		this.sec3CurrCode = sec3CurrCode;
	}

	public Integer getMainCurrCode() {
		return mainCurrCode;
	}

	public void setMainCurrCode(Integer mainCurrCode) {
		this.mainCurrCode = mainCurrCode;
	}

	public Integer getSecCurrCode() {
		return secCurrCode;
	}

	public void setSecCurrCode(Integer secCurrCode) {
		this.secCurrCode = secCurrCode;
	}

	public Integer getSec2CurrCode() {
		return sec2CurrCode;
	}

	public void setSec2CurrCode(Integer sec2CurrCode) {
		this.sec2CurrCode = sec2CurrCode;
	}

	public Integer getSec3CurrCode() {
		return sec3CurrCode;
	}

	public void setSec3CurrCode(Integer sec3CurrCode) {
		this.sec3CurrCode = sec3CurrCode;
	}
}
