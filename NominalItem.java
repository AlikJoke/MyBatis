package ru.bpc.cm.items.forecast.nominal;

import ru.bpc.cm.items.encashments.EncashmentCassItem;

public class NominalItem {

	private int cassNum;
	private int denom;
	private int currency;
	private int cassCount;
	private int countLast;
	private int countInOneCassPlan;
	private int maxCountInOneCass;
	private int minCountInOneCass;
	private int remainingOnEncDate;
	private double averageDemand;
	private double dayTakeOffCoeff;
	private double denomMultipleCoeff;
	
	public NominalItem(){}
	
	public NominalItem(NominalItem nominal){
		this.denom                   =  nominal.getDenom();             
		this.currency                =  nominal.getCurrency();          
		this.cassCount               =  nominal.getCassCount();         
		this.countLast               =  nominal.getCountLast();         
		this.countInOneCassPlan      =  nominal.getCountInOneCassPlan();
		this.maxCountInOneCass       =  nominal.getMaxCountInOneCass(); 
		this.minCountInOneCass       =  nominal.getMinCountInOneCass(); 
		this.remainingOnEncDate      =  nominal.getRemainingOnEncDate();
		this.averageDemand           =  nominal.getAverageDemand();     
		this.dayTakeOffCoeff         =  nominal.getDayTakeOffCoeff();   
		this.denomMultipleCoeff      =  nominal.getDenomMultipleCoeff();
	}
	
	public NominalItem(EncashmentCassItem cass){
		this.cassCount = 1;
		this.countInOneCassPlan = cass.getDenomCount();
		this.countLast = cass.getDenomCount();
		this.currency = cass.getDenomCurr();
		this.dayTakeOffCoeff = 1;
		this.denom = cass.getDenomValue();
		this.denomMultipleCoeff = 1;
	}
	
	public NominalItem(Integer denom, Integer countTrans, Double countDays, Double denomCount, Integer currency) {
		this.denom = denom;
		this.countLast = countTrans == 0 ? 1 : countTrans;
		this.dayTakeOffCoeff = countTrans / countDays;
		this.denomMultipleCoeff = countTrans == 0 ? 1 : denomCount / countTrans;
		this.cassCount = 1;
		this.currency = currency;
	}

	public void setDenom(int denom) {
	    this.denom = denom;
    }
	public int getDenom() {
	    return denom;
    }
	public void setCurrency(int currency) {
	    this.currency = currency;
    }
	public int getCurrency() {
	    return currency;
    }
	public void setCassCount(int cassCount) {
	    this.cassCount = cassCount;
    }
	public int getCassCount() {
	    return cassCount;
    }

	public void setCountLast(int countLast) {
	    this.countLast = countLast;
    }
	public int getCountLast() {
	    return countLast;
    }
	public void setDayTakeOffCoeff(double dayTakeOffCoeff) {
	    this.dayTakeOffCoeff = dayTakeOffCoeff;
    }
	public double getDayTakeOffCoeff() {
	    return dayTakeOffCoeff;
    }
	public void setDenomMultipleCoeff(double denomMultipleCoeff) {
	    this.denomMultipleCoeff = denomMultipleCoeff;
    }
	public double getDenomMultipleCoeff() {
	    return denomMultipleCoeff;
    }
	public void setCountInOneCassPlan(int countInOneCassPlan) {
	    this.countInOneCassPlan = countInOneCassPlan;
    }
	public int getCountInOneCassPlan() {
	    return countInOneCassPlan;
    }

	public int getMaxCountInOneCass() {
		return maxCountInOneCass;
	}

	public void setMaxCountInOneCass(int maxCountInOneCass) {
		this.maxCountInOneCass = maxCountInOneCass;
	}

	public int getRemainingOnEncDate() {
		return remainingOnEncDate;
	}

	public void setRemainingOnEncDate(int remainingOnEncDate) {
		this.remainingOnEncDate = remainingOnEncDate;
	}

	public int getMinCountInOneCass() {
		return minCountInOneCass;
	}

	public void setMinCountInOneCass(int minCountInOneCass) {
		this.minCountInOneCass = minCountInOneCass;
	}

	public double getAverageDemand() {
		return averageDemand;
	}

	public void setAverageDemand(double averageDemand) {
		this.averageDemand = averageDemand;
	}

	public int getCassNum() {
		return cassNum;
	}

	public void setCassNum(int cassNum) {
		this.cassNum = cassNum;
	}

}

