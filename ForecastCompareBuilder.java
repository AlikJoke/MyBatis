package ru.bpc.cm.cashmanagement.orm.builders;

import java.sql.Timestamp;
import java.util.Map;

import ru.bpc.cm.forecasting.anyatm.items.EncashmentForPeriod;
import ru.bpc.cm.forecasting.anyatm.items.ForecastForPeriod;

public class ForecastCompareBuilder {

	public String insertCompareDataBuilder(Map<String, Object> params) {
		String nextSequence = (String) params.get("nextSeq");
		EncashmentForPeriod encashment = (EncashmentForPeriod) params.get("encashment");
		ForecastForPeriod item = (ForecastForPeriod) params.get("item");
		return "Insert into T_CM_ENC_COMPARE " + " (ID, ATM_ID, DATE_FORTHCOMING_ENCASHMENT,  "
				+ " ENCASHMENT_TYPE,FORECAST_RESP_CODE,"
				+ " CASH_IN_EXISTS, EMERGENCY_ENCASHMENT,ENC_LOSTS,ENC_PRICE,ENC_LOSTS_CURR) " + " VALUES " + " ("
				+ nextSequence + ", " + item.getAtmId() + ", "
				+ new Timestamp(encashment.getForthcomingEncDate().getTime()) + ", " + encashment.getEncType().getId()
				+ ", " + encashment.getForecastResp() + ", " + item.isCashInExists() + ", "
				+ encashment.isEmergencyEncashment() + ", " + Math.round(encashment.getEncLosts()) + ", "
				+ Math.round(encashment.getEncPrice()) + ", " + encashment.getEncLostsCurrCode() + ")";
	}

	public String getPlanIdBuilder(Map<String, Object> params) {
		String currSequence = (String) params.get("currSeq");
		String from = (String) params.get("from");
		return "SELECT " + currSequence + " as SQ " + from;
	}
}
