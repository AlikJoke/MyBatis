package ru.bpc.cm.items.monitoring;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ru.bpc.cm.utils.CmUtils;


public class AtmActualStateItem implements Serializable {

    private static final long serialVersionUID = 1L;

    // for mybatis orm
    private String state;
    private String city;
    private String street;
    
    private int mainCurrRecRemaining;
    private int secCurrRecRemaining;
    private int sec2CurrRecRemaining;
    private int sec3CurrRecRemaining;
    
    private int mainCurrRemaining;
    private int secCurrRemaining;
    private int sec2CurrRemaining;
    private int sec3CurrRemaining;
    
    private int mainCurrCI;
    private int secCurrCI;
    private int sec2CurrCI;
    private int sec3CurrCI;
    
    private int mainCurrCILastHourDiff;
    private int secCurrCILastHourDiff;
    private int sec2CurrCILastHourDiff;
    private int sec3CurrCILastHourDiff;
    
    private int mainCurrCILastThreeHours;
    private int secCurrCILastThreeHours;
    private int sec2CurrCILastThreeHours;
    private int sec3CurrCILastThreeHours;
    
    private int mainCurrCO;
    private int secCurrCO;
    private int sec2CurrCO;
    private int sec3CurrCO;
    
    private int mainCurrCOLastHourDiff;
    private int secCurrCOLastHourDiff;
    private int sec2CurrCOLastHourDiff;
    private int sec3CurrCOLastHourDiff;
    
    private int mainCurrCOLastThreeHours;
    private int secCurrCOLastThreeHours;
    private int sec2CurrCOLastThreeHours;
    private int sec3CurrCOLastThreeHours;
    
    private int cashInState;
    // end
    
    private int atmID;//+

	private String extAtmId;//+
    private String desc;//+
    private List<AtmCashOutCassetteItem> cashOutCassettes = new ArrayList<AtmCashOutCassetteItem>();
    private List<AtmRecyclingCassetteItem> cashInRCassettes = new ArrayList<AtmRecyclingCassetteItem>();
    
    private int cashInInit;//!!!!
    private int cashInLeft;//+
    private String cashInClass;//+
    
    private int rejectInit;//!!!!
    private int rejectLeft;//+
    private int mainCurrCode = 0;//+
    private String mainCurrA3;
    private int secCurrCode = 0;//+
    private String secCurrA3;
    private int sec2CurrCode = 0;//+
    private String sec2CurrA3;
    private int sec2CurrRemains = 0;//+
    private int sec3CurrCode = 0;//+
    private String sec3CurrA3;
    private int sec3CurrRemains = 0;//+
    private Date statDate = null;
    private Date statLoadDate = null;
    private int encID = 0;
    private int cashInEncId = 0;
    private int cashInRInit;

    private Map<String,Integer> coRemainings;
    private Map<String,Integer> recRemainings;
    private Map<String,Integer> avgCashIn;
    private Map<String,Integer> avgCashOut;
    private Map<String,Integer> avgCashInLastThreeHours;
    private Map<String,Integer> avgCashOutLastThreeHours;
    private Map<String,Integer> avgCashInDifference;
    private Map<String,Integer> avgCashOutDifference;
    private Map<String,String> currs;

    private Date outOfCashOutDate;
    private String outOfCashOutCurr;
    private int outOfCashOutResp;
    
    private Date outOfCashInDate;
    private int outOfCashInResp;
    
    private int lastWithdrHours=0;
    private int lastAddHours=0;

    private String atmName;

    private Date dateForthcomingEncashment;
    private boolean emergencyEncashment;
    
    private int daysUntilEncashment;
    private boolean approved;
    
    private boolean currRemainingAlert= false;
    private boolean atmIsOk = true;
    
    private Integer maxBalanceDiff;
    
	public AtmActualStateItem(String state, String city, String street, Integer mainCurrRecRemaining,
			Integer secCurrRecRemaining, Integer sec2CurrRecRemaining, Integer sec3CurrRecRemaining,
			Integer mainCurrRemaining, Integer secCurrRemaining, Integer sec2CurrRemaining, Integer sec3CurrRemaining,
			Integer mainCurrCI, Integer secCurrCI, Integer sec2CurrCI, Integer sec3CurrCI,
			Integer mainCurrCILastHourDiff, Integer secCurrCILastHourDiff, Integer sec2CurrCILastHourDiff,
			Integer sec3CurrCILastHourDiff, Integer mainCurrCILastThreeHours, Integer secCurrCILastThreeHours,
			Integer sec2CurrCILastThreeHours, Integer sec3CurrCILastThreeHours, Integer mainCurrCO, Integer secCurrCO,
			Integer sec2CurrCO, Integer sec3CurrCO, Integer mainCurrCOLastHourDiff, Integer secCurrCOLastHourDiff,
			Integer sec2CurrCOLastHourDiff, Integer sec3CurrCOLastHourDiff, Integer mainCurrCOLastThreeHours,
			Integer secCurrCOLastThreeHours, Integer sec2CurrCOLastThreeHours, Integer sec3CurrCOLastThreeHours,
			Integer cashInState, Integer atmID, String extAtmId, Integer cashInInit, Integer cashInLeft,
			Integer rejectInit, Integer rejectLeft, Integer mainCurrCode, String mainCurrA3,
			Integer secCurrCode, String secCurrA3, Integer sec2CurrCode, String sec2CurrA3, Integer sec3CurrCode,
			String sec3CurrA3, Date statDate, Date statLoadDate, Integer encID, Integer cashInEncId,
			Integer cashInRInit, String outOfCashOutCurr, Integer outOfCashOutResp, Date outOfCashInDate,
			Integer outOfCashInResp, Integer lastWithdrHours, Integer lastAddHours, String atmName,
			Date dateForthcomingEncashment, Boolean emergencyEncashment, Integer daysUntilEncashment, Boolean approved,
			Boolean currRemainingAlert, Date outOfCashOutDate) {
		this();
		this.state = state;
		this.city = city;
		this.street = street;
		this.mainCurrRecRemaining = mainCurrRecRemaining == null ? 0 : mainCurrRecRemaining;
		this.secCurrRecRemaining = secCurrRecRemaining == null ? 0 : secCurrRecRemaining;
		this.sec2CurrRecRemaining = sec2CurrRecRemaining == null ? 0 : sec2CurrRecRemaining;
		this.sec3CurrRecRemaining = sec3CurrRecRemaining == null ? 0 : sec3CurrRecRemaining;
		this.mainCurrRemaining = mainCurrRemaining == null ? 0 : mainCurrRemaining;
		this.secCurrRemaining = secCurrRemaining == null ? 0 : secCurrRemaining;
		this.sec2CurrRemaining = sec2CurrRemaining == null ? 0 : sec2CurrRemaining;
		this.sec3CurrRemaining = sec3CurrRemaining == null ? 0 : sec3CurrRemaining;
		this.mainCurrCI = mainCurrCI == null ? 0 : mainCurrCI;
		this.secCurrCI = secCurrCI == null ? 0 : secCurrCI;
		this.sec2CurrCI = sec2CurrCI == null ? 0 : sec2CurrCI;
		this.sec3CurrCI = sec3CurrCI == null ? 0 : sec3CurrCI;
		this.mainCurrCILastHourDiff = mainCurrCILastHourDiff == null ? 0 : mainCurrCILastHourDiff;
		this.secCurrCILastHourDiff = secCurrCILastHourDiff == null ? 0 : secCurrCILastHourDiff;
		this.sec2CurrCILastHourDiff = sec2CurrCILastHourDiff == null ? 0 : sec2CurrCILastHourDiff;
		this.sec3CurrCILastHourDiff = sec3CurrCILastHourDiff == null ? 0 : sec3CurrCILastHourDiff;
		this.mainCurrCILastThreeHours = mainCurrCILastThreeHours == null ? 0 : mainCurrCILastThreeHours;
		this.secCurrCILastThreeHours = secCurrCILastThreeHours == null ? 0 : secCurrCILastThreeHours;
		this.sec2CurrCILastThreeHours = sec2CurrCILastThreeHours == null ? 0 : sec2CurrCILastThreeHours;
		this.sec3CurrCILastThreeHours = sec3CurrCILastThreeHours == null ? 0 : sec3CurrCILastThreeHours;
		this.mainCurrCO = mainCurrCO == null ? 0 : mainCurrCO;
		this.secCurrCO = secCurrCO == null ? 0 : secCurrCO;
		this.sec2CurrCO = sec2CurrCO == null ? 0 : sec2CurrCO;
		this.sec3CurrCO = sec3CurrCO == null ? 0 : sec3CurrCO;
		this.mainCurrCOLastHourDiff = mainCurrCOLastHourDiff == null ? 0 : mainCurrCOLastHourDiff;
		this.secCurrCOLastHourDiff = secCurrCOLastHourDiff == null ? 0 : secCurrCOLastHourDiff;
		this.sec2CurrCOLastHourDiff = sec2CurrCOLastHourDiff == null ? 0 : sec2CurrCOLastHourDiff;
		this.sec3CurrCOLastHourDiff = sec3CurrCOLastHourDiff == null ? 0 : sec3CurrCOLastHourDiff;
		this.mainCurrCOLastThreeHours = mainCurrCOLastThreeHours == null ? 0 : mainCurrCOLastThreeHours;
		this.secCurrCOLastThreeHours = secCurrCOLastThreeHours == null ? 0 : secCurrCOLastThreeHours;
		this.sec2CurrCOLastThreeHours = sec2CurrCOLastThreeHours == null ? 0 : sec2CurrCOLastThreeHours;
		this.sec3CurrCOLastThreeHours = sec3CurrCOLastThreeHours == null ? 0 : sec3CurrCOLastThreeHours;
		this.cashInState = cashInState == null ? 0 : cashInState;
		this.atmID = atmID;
		this.extAtmId = extAtmId;
		this.desc = CmUtils.getAtmFullAdrress(this.state, this.city, this.street);
		this.cashOutCassettes = new ArrayList<AtmCashOutCassetteItem>();
		this.cashInRCassettes = new ArrayList<AtmRecyclingCassetteItem>();
		this.cashInInit = cashInInit == null ? 0 : cashInInit;
		this.cashInLeft = cashInLeft == null ? 0 : cashInLeft;
		this.cashInClass = this.cashInState == 0 ? "ci" : "na";
		this.rejectInit = rejectInit == null ? 0 : rejectInit;
		this.rejectLeft = rejectLeft == null ? 0 : rejectLeft;
		this.mainCurrCode = mainCurrCode == null ? 0 : mainCurrCode;
		this.mainCurrA3 = mainCurrA3;
		this.secCurrCode = secCurrCode == null ? 0 : secCurrCode;
		this.secCurrA3 = secCurrA3;
		this.sec2CurrCode = sec2CurrCode == null ? 0 : sec2CurrCode;
		this.sec2CurrA3 = sec2CurrA3;
		this.sec3CurrCode = sec3CurrCode == null ? 0 : sec3CurrCode;
		this.sec3CurrA3 = sec3CurrA3;
		this.statDate = statDate;
		this.statLoadDate = statLoadDate;
		this.encID = encID == null ? 0 : encID;
		this.cashInEncId = cashInEncId == null ? 0 : cashInEncId;
		this.cashInRInit = cashInRInit == null ? 0 : cashInRInit;

		this.coRemainings.put(this.mainCurrA3, this.mainCurrRemaining);
		this.coRemainings.put(this.secCurrA3, this.secCurrRemaining);
		this.coRemainings.put(this.sec2CurrA3, this.sec2CurrRemaining);
		this.coRemainings.put(this.sec3CurrA3, this.sec3CurrRemaining);

		this.recRemainings.put(this.mainCurrA3, this.mainCurrRecRemaining);
		this.recRemainings.put(this.secCurrA3, this.secCurrRecRemaining);
		this.recRemainings.put(this.sec2CurrA3, this.sec2CurrRecRemaining);
		this.recRemainings.put(this.sec3CurrA3, this.sec3CurrRecRemaining);

		this.avgCashIn.put(this.mainCurrA3, this.mainCurrCI);
		this.avgCashIn.put(this.secCurrA3, this.secCurrCI);
		this.avgCashIn.put(this.sec2CurrA3, this.sec2CurrCI);
		this.avgCashIn.put(this.sec3CurrA3, this.sec3CurrCI);

		if (this.mainCurrCILastHourDiff != 0)
			this.avgCashInDifference.put(this.mainCurrA3, Math.abs(this.mainCurrCILastHourDiff));
		if (this.secCurrCILastHourDiff != 0)
			this.avgCashInDifference.put(this.secCurrA3, Math.abs(this.secCurrCILastHourDiff));
		if (this.sec2CurrCILastHourDiff != 0)
			this.avgCashInDifference.put(this.sec2CurrA3, Math.abs(this.sec2CurrCILastHourDiff));
		if (this.sec3CurrCILastHourDiff != 0)
			this.avgCashInDifference.put(this.sec3CurrA3, Math.abs(this.sec3CurrCILastHourDiff));

		this.avgCashInLastThreeHours.put(this.mainCurrA3, this.mainCurrCILastThreeHours);
		this.avgCashInLastThreeHours.put(this.secCurrA3, this.secCurrCILastThreeHours);
		this.avgCashInLastThreeHours.put(this.sec2CurrA3, this.sec2CurrCILastThreeHours);
		this.avgCashInLastThreeHours.put(this.sec3CurrA3, this.sec3CurrCILastThreeHours);

		this.avgCashOut.put(this.mainCurrA3, this.mainCurrCO);
		this.avgCashOut.put(this.secCurrA3, this.secCurrCO);
		this.avgCashOut.put(this.sec2CurrA3, this.sec2CurrCO);
		this.avgCashOut.put(this.sec3CurrA3, this.sec3CurrCO);

		this.avgCashOutLastThreeHours.put(this.mainCurrA3, this.mainCurrCOLastThreeHours);
		this.avgCashOutLastThreeHours.put(this.secCurrA3, this.secCurrCOLastThreeHours);
		this.avgCashOutLastThreeHours.put(this.sec2CurrA3, this.sec2CurrCOLastThreeHours);
		this.avgCashOutLastThreeHours.put(this.sec3CurrA3, this.sec3CurrCOLastThreeHours);

		if (this.mainCurrCOLastHourDiff != 0)
			this.avgCashOutDifference.put(this.mainCurrA3, Math.abs(this.mainCurrCOLastHourDiff));
		if (this.secCurrCOLastHourDiff != 0)
			this.avgCashOutDifference.put(this.secCurrA3, Math.abs(this.secCurrCOLastHourDiff));
		if (this.sec2CurrCOLastHourDiff != 0)
			this.avgCashOutDifference.put(this.sec2CurrA3, Math.abs(this.sec2CurrCOLastHourDiff));
		if (this.sec3CurrCOLastHourDiff != 0)
			this.avgCashOutDifference.put(this.sec3CurrA3, Math.abs(this.sec3CurrCOLastHourDiff));

		this.currs.put(new Integer(this.mainCurrCode).toString(), this.mainCurrA3);
		this.currs.put(new Integer(this.secCurrCode).toString(), this.secCurrA3);
		this.currs.put(new Integer(this.sec2CurrCode).toString(), this.sec2CurrA3);
		this.currs.put(new Integer(this.sec3CurrCode).toString(), this.sec3CurrA3);

		this.outOfCashOutDate = outOfCashOutDate;
		this.outOfCashOutCurr = outOfCashOutCurr;
		this.outOfCashOutResp = outOfCashOutResp == null ? 0 : outOfCashOutResp;
		this.outOfCashInDate = outOfCashInDate;
		this.outOfCashInResp = outOfCashInResp == null ? 0 : outOfCashInResp;
		this.lastWithdrHours = lastWithdrHours == null ? 0 : lastWithdrHours;
		this.lastAddHours = lastAddHours == null ? 0 : lastAddHours;
		this.atmName = atmName;
		this.dateForthcomingEncashment = dateForthcomingEncashment;
		this.emergencyEncashment = emergencyEncashment == null ? false : emergencyEncashment;
		this.daysUntilEncashment = daysUntilEncashment == null ? 0 : daysUntilEncashment;
		this.approved = approved == null ? false : approved;
		this.currRemainingAlert = currRemainingAlert == null ? false : currRemainingAlert;
	}
    
    public AtmActualStateItem() {
    	coRemainings = new HashMap<String, Integer>();
    	recRemainings = new HashMap<String, Integer>();
    	avgCashIn = new HashMap<String, Integer>();
    	avgCashOut = new HashMap<String, Integer>();
    	avgCashInLastThreeHours = new HashMap<String, Integer>(); 
    	avgCashOutLastThreeHours = new HashMap<String, Integer>();
    	avgCashInDifference = new HashMap<String, Integer>();
    	avgCashOutDifference = new HashMap<String, Integer>();
    	currs = new HashMap<String, String>();
	}

	public int getAtmID() {
    	return atmID;
    }
	public void setAtmID(int atmID) {
    	this.atmID = atmID;
    }
	public String getDesc() {
    	return desc;
    }
	public void setDesc(String desc) {
    	this.desc = desc;
    }
	public int getCashInInit() {
    	return cashInInit;
    }
	public void setCashInInit(int cashInInit) {
    	this.cashInInit = cashInInit;
    }
	public int getCashInLeft() {
    	return cashInLeft;
    }
	public void setCashInLeft(int cashInLeft) {
    	this.cashInLeft = cashInLeft;
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
	public int getSec2CurrRemains() {
    	return sec2CurrRemains;
    }
	public void setSec2CurrRemains(int sec2CurrRemains) {
    	this.sec2CurrRemains = sec2CurrRemains;
    }
	public int getSec3CurrCode() {
		return sec3CurrCode;
	}

	public void setSec3CurrCode(int sec3CurrCode) {
		this.sec3CurrCode = sec3CurrCode;
	}

	public String getSec3CurrA3() {
		return sec3CurrA3;
	}

	public void setSec3CurrA3(String sec3CurrA3) {
		this.sec3CurrA3 = sec3CurrA3;
	}

	public int getSec3CurrRemains() {
		return sec3CurrRemains;
	}

	public void setSec3CurrRemains(int sec3CurrRemains) {
		this.sec3CurrRemains = sec3CurrRemains;
	}
	
	public void setCashOutCassettes(List<AtmCashOutCassetteItem> cashOutCassettes) {
	    this.cashOutCassettes = cashOutCassettes;
    }
	public List<AtmCashOutCassetteItem> getCashOutCassettes() {
	    return cashOutCassettes;
    }
	public void setSec2CurrCode(int sec2CurrCode) {
	    this.sec2CurrCode = sec2CurrCode;
    }
	public int getSec2CurrCode() {
	    return sec2CurrCode;
    }
	public void setStatDate(Date statDate) {
	    this.statDate = statDate;
    }
	public Date getStatDate() {
	    return statDate;
    }
	public void setEncID(int encID) {
	    this.encID = encID;
    }
	public int getEncID() {
	    return encID;
    }

	public int getCassettesCount(){
		return this.getCashOutCassettes().size();
	}
	public void setMainCurrA3(String mainCurrA3) {
	    this.mainCurrA3 = mainCurrA3;
    }
	public String getMainCurrA3() {
	    return mainCurrA3;
    }
	public void setSecCurrA3(String secCurrA3) {
	    this.secCurrA3 = secCurrA3;
    }
	public String getSecCurrA3() {
	    return secCurrA3;
    }
	public void setSec2CurrA3(String sec2CurrA3) {
	    this.sec2CurrA3 = sec2CurrA3;
    }
	public String getSec2CurrA3() {
	    return sec2CurrA3;
    }
	public int getLastWithdrHours() {
		return lastWithdrHours;
	}
	public void setLastWithdrHours(int lastWithdrHours) {
		this.lastWithdrHours = lastWithdrHours;
	}
	public int getLastAddHours() {
		return lastAddHours;
	}
	public void setLastAddHours(int lastAddHours) {
		this.lastAddHours = lastAddHours;
	}
	public int getRejectLeft() {
		return rejectLeft;
	}
	public void setRejectLeft(int rejectLeft) {
		this.rejectLeft = rejectLeft;
	}
	public int getRejectInit() {
		return rejectInit;
	}
	public void setRejectInit(int rejectInit) {
		this.rejectInit = rejectInit;
	}
	public Date getStatLoadDate() {
		return statLoadDate;
	}
	public void setStatLoadDate(Date statLoadDate) {
		this.statLoadDate = statLoadDate;
	}
	public Date getOutOfCashInDate() {
		return outOfCashInDate;
	}
	public void setOutOfCashInDate(Date outOfCashInDate) {
		this.outOfCashInDate = outOfCashInDate;
	}
	public int getOutOfCashInResp() {
		return outOfCashInResp;
	}
	public void setOutOfCashInResp(int outOfCashInResp) {
		this.outOfCashInResp = outOfCashInResp;
	}
	public String getAtmName() {
		return atmName;
	}
	public void setAtmName(String atmName) {
		this.atmName = atmName;
	}
	public Date getDateForthcomingEncashment() {
		return dateForthcomingEncashment;
	}
	public void setDateForthcomingEncashment(Date dateForthcomingEncashment) {
		this.dateForthcomingEncashment = dateForthcomingEncashment;
	}
	public boolean isEmergencyEncashment() {
		return emergencyEncashment;
	}
	public void setEmergencyEncashment(boolean emergencyEncashment) {
		this.emergencyEncashment = emergencyEncashment;
	}
	public List<AtmRecyclingCassetteItem> getCashInRCassettes() {
		return cashInRCassettes;
	}
	public void setCashInRCassettes(List<AtmRecyclingCassetteItem> cashInRCassettes) {
		this.cashInRCassettes = cashInRCassettes;
	}
	public int getCashInEncId() {
		return cashInEncId;
	}
	public void setCashInEncId(int cashInEncId) {
		this.cashInEncId = cashInEncId;
	}
	public int getCashInRInit() {
		return cashInRInit;
	}
	public void setCashInRInit(int cashInRIntial) {
		this.cashInRInit = cashInRIntial;
	}
	public Map<String, Integer> getCoRemainings() {
		return coRemainings;
	}
	public void setCoRemainings(Map<String, Integer> coRemainings) {
		this.coRemainings = coRemainings;
	}
	public Map<String, Integer> getRecRemainings() {
		return recRemainings;
	}
	public void setRecRemainings(Map<String, Integer> recRemainings) {
		this.recRemainings = recRemainings;
	}
	public Map<String, Integer> getAvgCashIn() {
		return avgCashIn;
	}
	public void setAvgCashIn(Map<String, Integer> avgCashIn) {
		this.avgCashIn = avgCashIn;
	}
	public Map<String, Integer> getAvgCashOut() {
		return avgCashOut;
	}
	public void setAvgCashOut(Map<String, Integer> avgCashOut) {
		this.avgCashOut = avgCashOut;
	}
	public Date getOutOfCashOutDate() {
		return outOfCashOutDate;
	}
	public void setOutOfCashOutDate(Date outOfCashOutDate) {
		this.outOfCashOutDate = outOfCashOutDate;
	}
	public String getOutOfCashOutCurr() {
		return outOfCashOutCurr;
	}
	public void setOutOfCashOutCurr(String outOfCashOutCurr) {
		this.outOfCashOutCurr = outOfCashOutCurr;
	}
	public int getOutOfCashOutResp() {
		return outOfCashOutResp;
	}
	public void setOutOfCashOutResp(int outOfCashOutResp) {
		this.outOfCashOutResp = outOfCashOutResp;
	}

	public Map<String,String> getCurrs() {
		return currs;
	}

	public void setCurrs(Map<String,String> currs) {
		this.currs = currs;
	}

	public int getDaysUntilEncashment() {
		return daysUntilEncashment;
	}

	public void setDaysUntilEncashment(int daysUntilEncashment) {
		this.daysUntilEncashment = daysUntilEncashment;
	}

	public boolean getApproved() {
		return approved;
	}

	public void setApproved(boolean approved) {
		this.approved = approved;
	}

	public String getCashInClass() {
		return cashInClass;
	}

	public void setCashInClass(String cashInClass) {
		this.cashInClass = cashInClass;
	}

	public Map<String, Integer> getAvgCashInLastThreeHours() {
		return avgCashInLastThreeHours;
	}

	public void setAvgCashInLastThreeHours(
			Map<String, Integer> avgCashInLastThreeHours) {
		this.avgCashInLastThreeHours = avgCashInLastThreeHours;
	}

	public Map<String, Integer> getAvgCashOutLastThreeHours() {
		return avgCashOutLastThreeHours;
	}

	public void setAvgCashOutLastThreeHours(
			Map<String, Integer> avgCashOutLastThreeHours) {
		this.avgCashOutLastThreeHours = avgCashOutLastThreeHours;
	}

	public Map<String, Integer> getAvgCashInDifference() {
		return avgCashInDifference;
	}

	public void setAvgCashInDifference(Map<String, Integer> avgCashInDifference) {
		this.avgCashInDifference = avgCashInDifference;
	}

	public Map<String, Integer> getAvgCashOutDifference() {
		return avgCashOutDifference;
	}

	public void setAvgCashOutDifference(Map<String, Integer> avgCashOutDifference) {
		this.avgCashOutDifference = avgCashOutDifference;
	}
	
	public boolean isCashInExists(){
		return this.cashInEncId > 0;
	}
	
	public boolean isRecyclingExists(){
		return !this.cashInRCassettes.isEmpty(); 
		
	}

	public boolean isCurrRemainingAlert() {
		return currRemainingAlert;
	}

	public void setCurrRemainingAlert(boolean currRemainingAlert) {
		this.currRemainingAlert = currRemainingAlert;
	}

	public boolean isAtmIsOk() {
		return atmIsOk;
	}

	public void setAtmIsOk(boolean atmIsOk) {
		this.atmIsOk = atmIsOk;
	}

        public String getExtAtmId() {
		return extAtmId;
	}

	public void setExtAtmId(String extAtmId) {
		this.extAtmId = extAtmId;
	}
	
	public boolean isBalancesAlert() {
		boolean alertFlag = false;
		for (AtmCashOutCassetteItem item : this.cashOutCassettes){
			if (item.isBalanceAlert()){
				alertFlag = true;
				break;
			}
		}
		for (AtmRecyclingCassetteItem item : this.cashInRCassettes){
			if (item.isBalanceAlert()){
				alertFlag = true;
				break;
			}
		}
		
		return alertFlag;
	}
	
	public int getMaxBalanceDiff() {
		if (maxBalanceDiff==null){
			maxBalanceDiff = getBalanceDiff();
		}
		return maxBalanceDiff;
	}

	private int getBalanceDiff(){
			int temp = 0;
			int maxDiff = 0;
			for (AtmCashOutCassetteItem item : cashOutCassettes){
				temp=Math.abs(Math.abs(item.getAmountLeft())-Math.abs(item.getAmountLeftFE()));
				if (temp>maxDiff) {
					maxDiff = temp;
				}
			}
			
			for (AtmRecyclingCassetteItem item : cashInRCassettes){
				temp=Math.abs(Math.abs(item.getAmountLeft())-Math.abs(item.getAmountLeftFE()));
				if (temp>maxDiff) {
					maxDiff = temp;
				}
			}
			
			return maxDiff;
	}
}
