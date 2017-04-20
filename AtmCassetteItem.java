package ru.bpc.cm.items.monitoring;

import ru.bpc.cm.items.enums.AtmCassetteType;

public class AtmCassetteItem {

	private int number;
	private int denom;
	private int curr;
	private int capacity;
	private AtmCassetteType type;
	protected boolean balanceAlert;
	
	private boolean capacityResetNeeded = false;
	
	public AtmCassetteItem() {
		super();
	}
	
	public AtmCassetteItem(int number, int denom, int capacity, int curr, AtmCassetteType type, boolean balanceAlert) {
		this.number = number;
		this.denom = denom;
		this.capacity = capacity;
		this.curr = curr;
		this.type = type;
		this.balanceAlert = balanceAlert;
	}
	
	public AtmCassetteItem(int number, int denom, int curr, AtmCassetteType type, boolean balanceAlert) {
		this.number = number;
		this.denom = denom;
		this.curr = curr;
		this.type = type;
		this.balanceAlert = balanceAlert;
		this.capacity = 0;
	}
	
	public AtmCassetteItem(int number, int denom, int curr, AtmCassetteType type) {
		this.number = number;
		this.denom = denom;
		this.curr = curr;
		this.type = type;
		this.balanceAlert = false;
		this.capacity = 0;
	}
	
	public int getNumber() {
		return number;
	}
	public void setNumber(int number) {
		this.number = number;
	}
	public int getDenom() {
		return denom;
	}
	public void setDenom(int denom) {
		this.denom = denom;
	}
	public int getCapacity() {
		return capacity;
	}

	public void setCapacity(int capacity) {
		this.capacity = capacity;
	}

	public int getCurr() {
		return curr;
	}
	public void setCurr(int curr) {
		this.curr = curr;
	}
	public AtmCassetteType getType() {
		return type;
	}
	public void setType(AtmCassetteType type) {
		this.type = type;
	}
	
	protected boolean isBalanceAlert() {
		return balanceAlert;
	}

	protected void setBalanceAlert(boolean balanceAlert) {
		this.balanceAlert = balanceAlert;
	}

	public boolean isCapacityResetNeeded() {
		return capacityResetNeeded;
	}

	public void setCapacityResetNeeded(boolean capacityResetNeeded) {
		this.capacityResetNeeded = capacityResetNeeded;
	}
	
}
