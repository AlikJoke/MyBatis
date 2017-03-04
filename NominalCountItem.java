package ru.bpc.cm.cashmanagement.orm.items;

public class NominalCountItem {

	private Integer countTrans;
	private Integer denomCount;
	private Double countDays;

	public Integer getCountTrans() {
		return countTrans;
	}

	public void setCountTrans(Integer countTrans) {
		this.countTrans = countTrans;
	}

	public Integer getDenomCount() {
		return denomCount;
	}

	public void setDenomCount(Integer denomCount) {
		this.denomCount = denomCount;
	}

	public Double getCountDays() {
		return countDays;
	}

	public void setCountDays(Double countDays) {
		this.countDays = countDays;
	}

	public NominalCountItem(Integer countTrans, Integer denomCount, Double countDays) {
		super();
		this.countTrans = countTrans;
		this.denomCount = denomCount;
		this.countDays = countDays;
	}
}
