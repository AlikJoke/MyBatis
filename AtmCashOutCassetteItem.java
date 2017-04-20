package ru.bpc.cm.items.monitoring;

import java.io.Serializable;

import ru.bpc.cm.items.enums.AtmCassetteType;

public class AtmCashOutCassetteItem extends AtmCassetteItem implements Serializable {

	private static final long serialVersionUID = 1L;

	private int amountLeft;
	private int amountLeftFE;
	private int amountInit;
	private String codeA3;
	private String pbClass;

	private boolean emergencyBillsWarning;

	public AtmCashOutCassetteItem() {
		super();
	}

	public AtmCashOutCassetteItem(int number, int denom, int capacity, int curr, boolean balanceAlert) {
		super(number, denom, capacity, curr, AtmCassetteType.CASH_OUT_CASS, balanceAlert);
	}

	public AtmCashOutCassetteItem(int number, int denom, int curr, boolean balanceAlert) {
		super(number, denom, curr, AtmCassetteType.CASH_OUT_CASS, balanceAlert);
	}

	public void setAmountLeft(int amountLeft) {
		this.amountLeft = amountLeft;
	}

	public int getAmountLeft() {
		return amountLeft;
	}

	public int getAmountLeftFE() {
		return amountLeftFE;
	}

	public void setAmountLeftFE(int amountLeftFE) {
		this.amountLeftFE = amountLeftFE;
	}

	public void setAmountInit(int amountInit) {
		this.amountInit = amountInit;
	}

	public int getAmountInit() {
		return amountInit;
	}

	public void setCodeA3(String codeA3) {
		this.codeA3 = codeA3;
	}

	public String getCodeA3() {
		return codeA3;
	}

	public void setPbClass(String pbClass) {
		this.pbClass = pbClass;
	}

	public String getPbClass() {
		return pbClass;
	}

	public boolean isBalanceAlert() {
		return super.isBalanceAlert();
	}

	public void setBalanceAlert(boolean balanceAlert) {
		super.setBalanceAlert(balanceAlert);
	}

	public boolean isEmergencyBillsWarning() {
		return emergencyBillsWarning;
	}

	public void setEmergencyBillsWarning(boolean emergencyBillsWarning) {
		this.emergencyBillsWarning = emergencyBillsWarning;
	}
}
