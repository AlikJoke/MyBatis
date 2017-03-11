package ru.bpc.cm.cashmanagement.orm.builders;

import java.util.Map;

public class ForecastCompareBuilder {

	public String insertCompareDataBuilder(Map<String, Object> params) {
		String nextSequence = (String) params.get("nextSeq");
		return "Insert into T_CM_ENC_COMPARE " + " (ID, ATM_ID, DATE_FORTHCOMING_ENCASHMENT,  "
				+ " ENCASHMENT_TYPE,FORECAST_RESP_CODE,"
				+ " CASH_IN_EXISTS, EMERGENCY_ENCASHMENT,ENC_LOSTS,ENC_PRICE,ENC_LOSTS_CURR) " + " VALUES " + " ("
				+ nextSequence
				+ ", #{atmId}, #{encDate}, #{encType}, #{resp}, #{isExists}, #{isEmergency}, #{encLosts}, #{encPrice}, #{encLostsCurrCode})";
	}

	public String getPlanIdBuilder(Map<String, Object> params) {
		String currSequence = (String) params.get("currSeq");
		String from = (String) params.get("from");
		return "SELECT " + currSequence + " as SQ " + from;
	}
}
