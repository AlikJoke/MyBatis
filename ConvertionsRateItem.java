package ru.bpc.cm.cashmanagement.orm.items;

public class ConvertionsRateItem {

	private String srcCurrCode;
	private String destCurrCode;
	private String multipleFlag;
	private Double cnvtRate;

	public String getSrcCurrCode() {
		return srcCurrCode;
	}

	public void setSrcCurrCode(String srcCurrCode) {
		this.srcCurrCode = srcCurrCode;
	}

	public String getDestCurrCode() {
		return destCurrCode;
	}

	public void setDestCurrCode(String destCurrCode) {
		this.destCurrCode = destCurrCode;
	}

	public String getMultipleFlag() {
		return multipleFlag;
	}

	public void setMultipleFlag(String multipleFlag) {
		this.multipleFlag = multipleFlag;
	}

	public Double getCnvtRate() {
		return cnvtRate;
	}

	public void setCnvtRate(Double cnvtRate) {
		this.cnvtRate = cnvtRate;
	}

	public ConvertionsRateItem(String srcCurrCode, String destCurrCode, String multipleFlag, Double cnvtRate) {
		super();
		this.srcCurrCode = srcCurrCode;
		this.destCurrCode = destCurrCode;
		this.multipleFlag = multipleFlag;
		this.cnvtRate = cnvtRate;
	}

}
