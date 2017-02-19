package ru.bpc.cm.cashmanagement.orm.builders;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import ru.bpc.cm.filters.MonitoringFilter;
import ru.bpc.cm.items.enums.EncashmentFilterMode;
import ru.bpc.cm.utils.TextUtil;
import ru.bpc.cm.utils.db.DbTextUtil;
import ru.bpc.cm.utils.db.JdbcUtils;
import ru.bpc.cm.utils.db.QueryConstructor;

public class AtmActualStateBuilder {

	public String builderQueryCheckLoadedBalances(Map<String, Object> params) {
		Integer threshhold = (Integer) params.get("threshhold");
		@SuppressWarnings("unchecked")
		List<Integer> atmList = (List<Integer>) params.get("atmList");
		StringBuilder sb = new StringBuilder();
		sb.append("select ATM_ID, CASS_TYPE, CASS_NUMBER, CASS_REMAINING_LOAD, CASS_REMAINING_CALC "
				+ "from t_cm_intgr_cass_balance "
				+ "WHERE abs(COALESCE(CASS_REMAINING_LOAD,0) - COALESCE(CASS_REMAINING_CALC,0)) > ");
		sb.append(threshhold);
		sb.append(" AND BALANCE_STATUS <> 1 and ");
		sb.append(JdbcUtils.generateInConditionNumber("atm_id", atmList));
		sb.append(" ORDER BY ATM_ID, CASS_TYPE, CASS_NUMBER");
		return sb.toString();
	}

	public String builderAtmActualStateQuery(Map<String, MonitoringFilter> params) {
		MonitoringFilter addFilter = params.get("addFilter");
		StringBuilder sql = new StringBuilder("SELECT "
				+ "ac.ATM_ID, ac.EXTERNAL_ATM_ID, CASH_OUT_ENC_ID, CASH_OUT_STAT_DATE,CASH_IN_ENC_ID, "
				+ "COALESCE(CASH_IN_BILLS_REMAINING,0) as CASH_IN_BILLS_REMAINING, CASH_IN_BILLS_INITIAL,CASH_IN_R_BILLS_INITIAL, "
				+ "COALESCE(REJECT_BILLS_REMAINING,0) as REJECT_BILLS_REMAINING, REJECT_BILLS_INITIAL, "
				+ "STATE, CITY, STREET, "
				+ "MAIN_CURR_REMAINING, SEC_CURR_REMAINING, SEC2_CURR_REMAINING, SEC3_CURR_REMAINING,"
				+ "MAIN_CURR_REC_REMAINING, SEC_CURR_REC_REMAINING, SEC2_CURR_REC_REMAINING, SEC3_CURR_REC_REMAINING,"
				+ "MAIN_CURR_CODE, SECONDARY_CURR_CODE, SECONDARY2_CURR_CODE, SECONDARY3_CURR_CODE,"
				+ "COALESCE(MAIN_CODE_A3,'NOT_DEFINED') as MAIN_CODE_A3, "
				+ "SECONDARY_CODE_A3, SECONDARY2_CODE_A3, SECONDARY3_CODE_A3,"
				+ "OUT_OF_CASH_OUT_DATE, OUT_OF_CASH_OUT_CURR, OUT_OF_CASH_OUT_RESP,"
				+ "OUT_OF_CASH_IN_DATE, OUT_OF_CASH_IN_RESP, " + "STAT_LOAD_DATE, ATM_NAME , "
				+ "LAST_WITHDRAWAL_HOURS, LAST_ADDITION_HOURS," + "EMERGENCY_ENCASHMENT, DATE_FORTHCOMING_ENCASHMENT,"
				+ "IS_APPROVED, DAYS_UNTIL_ENCASHMENT, CASH_IN_STATE, CURR_REMAINING_ALERT, "
				+ "MAIN_CURR_CI,MAIN_CURR_CO,MAIN_CURR_CI_LAST_HOUR_DIFF,MAIN_CURR_CO_LAST_HOUR_DIFF,MAIN_CURR_CI_LAST_THREE_HOURS,MAIN_CURR_CO_LAST_THREE_HOURS, "
				+ "SEC_CURR_CI,SEC_CURR_CO,SEC_CURR_CI_LAST_HOUR_DIFF,SEC_CURR_CO_LAST_HOUR_DIFF,SEC_CURR_CI_LAST_THREE_HOURS,SEC_CURR_CO_LAST_THREE_HOURS, "
				+ "SEC2_CURR_CI,SEC2_CURR_CO,SEC2_CURR_CI_LAST_HOUR_DIFF,SEC2_CURR_CO_LAST_HOUR_DIFF,SEC2_CURR_CI_LAST_THREE_HOURS,SEC2_CURR_CO_LAST_THREE_HOURS, "
				+ "SEC3_CURR_CI,SEC3_CURR_CO,SEC3_CURR_CI_LAST_HOUR_DIFF,SEC3_CURR_CO_LAST_HOUR_DIFF,SEC3_CURR_CI_LAST_THREE_HOURS,SEC3_CURR_CO_LAST_THREE_HOURS "
				+ "FROM V_CM_ATM_ACTUAL_STATE ac " + "join T_CM_ATM_AVG_DEMAND ad on (ac.ATM_ID = ad.ATM_ID) "
				+ "WHERE ac.atm_id in (select id from t_cm_temp_atm_list) ");

		switch (addFilter.getEncashmenFilterMode()) {
		case ANY:
			break;
		case EMERGENCY:
			sql.append(" AND (DATE_FORTHCOMING_ENCASHMENT IS NOT NULL AND EMERGENCY_ENCASHMENT > 0) ");
			break;
		case NONE:
			sql.append(" AND DATE_FORTHCOMING_ENCASHMENT IS NULL ");
			break;
		case STANDARD:
			sql.append(" AND (DATE_FORTHCOMING_ENCASHMENT IS NOT NULL AND EMERGENCY_ENCASHMENT = 0) ");
			break;
		default:
			break;
		}
		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql.toString(), true);

		querConstr.addElementIfNotNull("atm_id", "AND", "ac.EXTERNAL_ATM_ID",
				DbTextUtil.getOperationType(addFilter.getAtmID()), addFilter.getAtmID());

		if (addFilter.getEncashmenFilterMode() != EncashmentFilterMode.NONE) {
			querConstr.addElementIfNotNull("date_forthcoming_enc", "AND", "DATE_FORTHCOMING_ENCASHMENT", ">=",
					addFilter.getForthcomingEncDateFrom());
			querConstr.addElementIfNotNull("date_forthcoming_enc", "AND", "DATE_FORTHCOMING_ENCASHMENT", "<=",
					addFilter.getForthcomingEncDateTo());
		}

		if (TextUtil.isNotEmpty(addFilter.getNameAndAddress())) {
			querConstr.addElement("atm_name", "AND ( ", "ATM_NAME",
					DbTextUtil.getOperationType(addFilter.getNameAndAddress()), addFilter.getNameAndAddress(), false);
			querConstr.addElement("address", "OR", "address",
					DbTextUtil.getOperationType(addFilter.getNameAndAddress()), addFilter.getNameAndAddress(), false);
			querConstr.addSimpleExpression("atm_name_addr", ")", " ");
		}

		if (addFilter.getTypeByOperations() > -1) {
			querConstr.addElementInt("atm_type", "AND", "COALESCE(ATM_TYPE,0)", "=", addFilter.getTypeByOperations());
		}
		if (addFilter.getExhaustDateFrom() != null || addFilter.getExhaustDateTo() != null) {
			querConstr.addSimpleExpression("out_of_dates", "AND ((", " ");

			querConstr.addElementIfNotNull("out_of_curr_date", " ", "OUT_OF_CASH_OUT_DATE", ">=",
					addFilter.getExhaustDateFrom());
			if (addFilter.getExhaustDateFrom() != null && addFilter.getExhaustDateTo() != null) {
				querConstr.addSimpleExpression("out_of_dates", "AND", " ");
			}
			querConstr.addElementIfNotNull("out_of_curr_date", " ", "OUT_OF_CASH_OUT_DATE", "<=",
					addFilter.getExhaustDateTo());

			querConstr.addSimpleExpression("out_of_dates", ") OR (", " ");

			querConstr.addElementIfNotNull("out_of_cashin_date", " ", "OUT_OF_CASH_IN_DATE", ">=",
					addFilter.getExhaustDateFrom());
			if (addFilter.getExhaustDateFrom() != null && addFilter.getExhaustDateTo() != null) {
				querConstr.addSimpleExpression("out_of_dates", "AND", " ");
			}
			querConstr.addElementIfNotNull("out_of_cashin_date", " ", "OUT_OF_CASH_IN_DATE", "<=",
					addFilter.getExhaustDateTo());
			querConstr.addSimpleExpression("out_of_dates", "))", " ");

		}

		querConstr.setQueryTail(" ORDER BY EXTERNAL_ATM_ID");
		try {
			return querConstr.getQuery();
		} catch (SQLException e) {
			throw new RuntimeException("Can't create valid query", e);
		}
	}
}
