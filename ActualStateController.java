package ru.bpc.cm.monitoring;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.bpc.cm.cashmanagement.CurrencyConverter;
import ru.bpc.cm.filters.MonitoringFilter;
import ru.bpc.cm.forecasting.anyatm.items.AnyAtmForecast;
import ru.bpc.cm.forecasting.anyatm.items.Currency;
import ru.bpc.cm.items.enums.AtmCassetteType;
import ru.bpc.cm.items.enums.EncashmentFilterMode;
import ru.bpc.cm.items.forecast.ForecastStatDay;
import ru.bpc.cm.items.monitoring.AtmActualStateItem;
import ru.bpc.cm.items.monitoring.AtmCashOutCassetteItem;
import ru.bpc.cm.items.monitoring.AtmCassetteItem;
import ru.bpc.cm.items.monitoring.AtmRecyclingCassetteItem;
import ru.bpc.cm.items.monitoring.MonitoringException;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.TextUtil;
import ru.bpc.cm.utils.db.DbTextUtil;
import ru.bpc.cm.utils.db.JdbcUtils;
import ru.bpc.cm.utils.db.QueryConstructor;

public class ActualStateController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");
	private static final String CASS_NA = "na";
	private static final String CASS_CI = "ci";
	private static final double ALERT_DELTA_LOW = 30000;
	private static final double ALERT_DELTA_HIGH = 300000;
	private static final int ALERT_DELTA_CURR = 810;
	private static final int BALANCES_CHECK_THRESHHOLD = 200;
	private static final String BALANCES_CHECK_ERROR_FORMAT = "Wrong balance for ATM: ID={}; CASS_TYPE = {}; CASS_NUMBER = {}; LOADED_REMAINING = {}; CALCULATED_REMAINING = {}";

	public String builderAtmActualStateQuery(MonitoringFilter addFilter) {
		StringBuilder sql = new StringBuilder("SELECT " +
				"ac.ATM_ID, ac.EXTERNAL_ATM_ID, CASH_OUT_ENC_ID, CASH_OUT_STAT_DATE,CASH_IN_ENC_ID, " +
				"COALESCE(CASH_IN_BILLS_REMAINING,0) as CASH_IN_BILLS_REMAINING, CASH_IN_BILLS_INITIAL,CASH_IN_R_BILLS_INITIAL, " +
				"COALESCE(REJECT_BILLS_REMAINING,0) as REJECT_BILLS_REMAINING, REJECT_BILLS_INITIAL, "+
				"STATE, CITY, STREET, " +
				"MAIN_CURR_REMAINING, SEC_CURR_REMAINING, SEC2_CURR_REMAINING, SEC3_CURR_REMAINING,"+
				"MAIN_CURR_REC_REMAINING, SEC_CURR_REC_REMAINING, SEC2_CURR_REC_REMAINING, SEC3_CURR_REC_REMAINING,"+
				"MAIN_CURR_CODE, SECONDARY_CURR_CODE, SECONDARY2_CURR_CODE, SECONDARY3_CURR_CODE," +
				"COALESCE(MAIN_CODE_A3,'NOT_DEFINED') as MAIN_CODE_A3, "+
				"SECONDARY_CODE_A3, SECONDARY2_CODE_A3, SECONDARY3_CODE_A3," +
				"OUT_OF_CASH_OUT_DATE, OUT_OF_CASH_OUT_CURR, OUT_OF_CASH_OUT_RESP,"+
				"OUT_OF_CASH_IN_DATE, OUT_OF_CASH_IN_RESP, " +
				"STAT_LOAD_DATE, ATM_NAME , " +
				"LAST_WITHDRAWAL_HOURS, LAST_ADDITION_HOURS," +
				"EMERGENCY_ENCASHMENT, DATE_FORTHCOMING_ENCASHMENT," +
				"IS_APPROVED, DAYS_UNTIL_ENCASHMENT, CASH_IN_STATE, CURR_REMAINING_ALERT, " +
				"MAIN_CURR_CI,MAIN_CURR_CO,MAIN_CURR_CI_LAST_HOUR_DIFF,MAIN_CURR_CO_LAST_HOUR_DIFF,MAIN_CURR_CI_LAST_THREE_HOURS,MAIN_CURR_CO_LAST_THREE_HOURS, "+
				"SEC_CURR_CI,SEC_CURR_CO,SEC_CURR_CI_LAST_HOUR_DIFF,SEC_CURR_CO_LAST_HOUR_DIFF,SEC_CURR_CI_LAST_THREE_HOURS,SEC_CURR_CO_LAST_THREE_HOURS, "+
				"SEC2_CURR_CI,SEC2_CURR_CO,SEC2_CURR_CI_LAST_HOUR_DIFF,SEC2_CURR_CO_LAST_HOUR_DIFF,SEC2_CURR_CI_LAST_THREE_HOURS,SEC2_CURR_CO_LAST_THREE_HOURS, " +
				"SEC3_CURR_CI,SEC3_CURR_CO,SEC3_CURR_CI_LAST_HOUR_DIFF,SEC3_CURR_CO_LAST_HOUR_DIFF,SEC3_CURR_CI_LAST_THREE_HOURS,SEC3_CURR_CO_LAST_THREE_HOURS " +
				"FROM V_CM_ATM_ACTUAL_STATE ac " +
				"join T_CM_ATM_AVG_DEMAND ad on (ac.ATM_ID = ad.ATM_ID) " +
				"WHERE ac.atm_id in (select id from t_cm_temp_atm_list) ");
			
			switch(addFilter.getEncashmenFilterMode()){
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

			querConstr.addElementIfNotNull("atm_id", "AND", "ac.EXTERNAL_ATM_ID", DbTextUtil.getOperationType(addFilter.getAtmID()),
					addFilter.getAtmID());
			
			if(addFilter.getEncashmenFilterMode() != EncashmentFilterMode.NONE){
				querConstr.addElementIfNotNull("date_forthcoming_enc", "AND", "DATE_FORTHCOMING_ENCASHMENT", ">=",
						addFilter.getForthcomingEncDateFrom());
				querConstr.addElementIfNotNull("date_forthcoming_enc", "AND", "DATE_FORTHCOMING_ENCASHMENT", "<=",
						addFilter.getForthcomingEncDateTo());
			}
			
			if(TextUtil.isNotEmpty(addFilter.getNameAndAddress())){
				querConstr.addElement("atm_name", "AND ( ", "ATM_NAME", DbTextUtil.getOperationType(addFilter.getNameAndAddress()),
						addFilter.getNameAndAddress(), false);
				querConstr.addElement("address", "OR", "address", DbTextUtil.getOperationType(addFilter.getNameAndAddress()),
						addFilter.getNameAndAddress(), false);
				querConstr.addSimpleExpression("atm_name_addr", ")", " ");
			}
			
			if(addFilter.getTypeByOperations() > -1){
				querConstr.addElementInt("atm_type", "AND", "COALESCE(ATM_TYPE,0)", "=", addFilter.getTypeByOperations());
			}
			if(addFilter.getExhaustDateFrom() != null || addFilter.getExhaustDateTo() != null){
				querConstr.addSimpleExpression("out_of_dates", "AND ((", " ");
				
				querConstr.addElementIfNotNull("out_of_curr_date", " ", "OUT_OF_CASH_OUT_DATE", ">=",
						addFilter.getExhaustDateFrom());
				if(addFilter.getExhaustDateFrom() != null && addFilter.getExhaustDateTo() != null){
					querConstr.addSimpleExpression("out_of_dates", "AND", " ");
				}
				querConstr.addElementIfNotNull("out_of_curr_date", " ", "OUT_OF_CASH_OUT_DATE", "<=",
						addFilter.getExhaustDateTo());
				
				querConstr.addSimpleExpression("out_of_dates", ") OR (", " ");
				
				
				querConstr.addElementIfNotNull("out_of_cashin_date", " ", "OUT_OF_CASH_IN_DATE", ">=",
						addFilter.getExhaustDateFrom());
				if(addFilter.getExhaustDateFrom() != null && addFilter.getExhaustDateTo() != null){
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
	
	public static List<AtmActualStateItem> getAtmActualStateList(Connection con, MonitoringFilter addFilter) throws SQLException {
		List<AtmActualStateItem> atmActualStateList = new ArrayList<AtmActualStateItem>();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {			
			StringBuilder sql = new StringBuilder("SELECT " +
				"ac.ATM_ID, ac.EXTERNAL_ATM_ID, CASH_OUT_ENC_ID, CASH_OUT_STAT_DATE,CASH_IN_ENC_ID, " +
				"COALESCE(CASH_IN_BILLS_REMAINING,0) as CASH_IN_BILLS_REMAINING, CASH_IN_BILLS_INITIAL,CASH_IN_R_BILLS_INITIAL, " +
				"COALESCE(REJECT_BILLS_REMAINING,0) as REJECT_BILLS_REMAINING, REJECT_BILLS_INITIAL, "+
				"STATE, CITY, STREET, " +
				"MAIN_CURR_REMAINING, SEC_CURR_REMAINING, SEC2_CURR_REMAINING, SEC3_CURR_REMAINING,"+
				"MAIN_CURR_REC_REMAINING, SEC_CURR_REC_REMAINING, SEC2_CURR_REC_REMAINING, SEC3_CURR_REC_REMAINING,"+
				"MAIN_CURR_CODE, SECONDARY_CURR_CODE, SECONDARY2_CURR_CODE, SECONDARY3_CURR_CODE," +
				"COALESCE(MAIN_CODE_A3,'NOT_DEFINED') as MAIN_CODE_A3, "+
				"SECONDARY_CODE_A3, SECONDARY2_CODE_A3, SECONDARY3_CODE_A3," +
				"OUT_OF_CASH_OUT_DATE, OUT_OF_CASH_OUT_CURR, OUT_OF_CASH_OUT_RESP,"+
				"OUT_OF_CASH_IN_DATE, OUT_OF_CASH_IN_RESP, " +
				"STAT_LOAD_DATE, ATM_NAME , " +
				"LAST_WITHDRAWAL_HOURS, LAST_ADDITION_HOURS," +
				"EMERGENCY_ENCASHMENT, DATE_FORTHCOMING_ENCASHMENT," +
				"IS_APPROVED, DAYS_UNTIL_ENCASHMENT, CASH_IN_STATE, CURR_REMAINING_ALERT, " +
				"MAIN_CURR_CI,MAIN_CURR_CO,MAIN_CURR_CI_LAST_HOUR_DIFF,MAIN_CURR_CO_LAST_HOUR_DIFF,MAIN_CURR_CI_LAST_THREE_HOURS,MAIN_CURR_CO_LAST_THREE_HOURS, "+
				"SEC_CURR_CI,SEC_CURR_CO,SEC_CURR_CI_LAST_HOUR_DIFF,SEC_CURR_CO_LAST_HOUR_DIFF,SEC_CURR_CI_LAST_THREE_HOURS,SEC_CURR_CO_LAST_THREE_HOURS, "+
				"SEC2_CURR_CI,SEC2_CURR_CO,SEC2_CURR_CI_LAST_HOUR_DIFF,SEC2_CURR_CO_LAST_HOUR_DIFF,SEC2_CURR_CI_LAST_THREE_HOURS,SEC2_CURR_CO_LAST_THREE_HOURS, " +
				"SEC3_CURR_CI,SEC3_CURR_CO,SEC3_CURR_CI_LAST_HOUR_DIFF,SEC3_CURR_CO_LAST_HOUR_DIFF,SEC3_CURR_CI_LAST_THREE_HOURS,SEC3_CURR_CO_LAST_THREE_HOURS " +
				"FROM V_CM_ATM_ACTUAL_STATE ac " +
				"join T_CM_ATM_AVG_DEMAND ad on (ac.ATM_ID = ad.ATM_ID) " +
				"WHERE ac.atm_id in (select id from t_cm_temp_atm_list) ");
			
			switch(addFilter.getEncashmenFilterMode()){
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

			querConstr.addElementIfNotNull("atm_id", "AND", "ac.EXTERNAL_ATM_ID", DbTextUtil.getOperationType(addFilter.getAtmID()),
					addFilter.getAtmID());
			
			if(addFilter.getEncashmenFilterMode() != EncashmentFilterMode.NONE){
				querConstr.addElementIfNotNull("date_forthcoming_enc", "AND", "DATE_FORTHCOMING_ENCASHMENT", ">=",
						addFilter.getForthcomingEncDateFrom());
				querConstr.addElementIfNotNull("date_forthcoming_enc", "AND", "DATE_FORTHCOMING_ENCASHMENT", "<=",
						addFilter.getForthcomingEncDateTo());
			}
			
			if(TextUtil.isNotEmpty(addFilter.getNameAndAddress())){
				querConstr.addElement("atm_name", "AND ( ", "ATM_NAME", DbTextUtil.getOperationType(addFilter.getNameAndAddress()),
						addFilter.getNameAndAddress(), false);
				querConstr.addElement("address", "OR", "address", DbTextUtil.getOperationType(addFilter.getNameAndAddress()),
						addFilter.getNameAndAddress(), false);
				querConstr.addSimpleExpression("atm_name_addr", ")", " ");
			}
			
			if(addFilter.getTypeByOperations() > -1){
				querConstr.addElementInt("atm_type", "AND", "COALESCE(ATM_TYPE,0)", "=", addFilter.getTypeByOperations());
			}
			if(addFilter.getExhaustDateFrom() != null || addFilter.getExhaustDateTo() != null){
				querConstr.addSimpleExpression("out_of_dates", "AND ((", " ");
				
				querConstr.addElementIfNotNull("out_of_curr_date", " ", "OUT_OF_CASH_OUT_DATE", ">=",
						addFilter.getExhaustDateFrom());
				if(addFilter.getExhaustDateFrom() != null && addFilter.getExhaustDateTo() != null){
					querConstr.addSimpleExpression("out_of_dates", "AND", " ");
				}
				querConstr.addElementIfNotNull("out_of_curr_date", " ", "OUT_OF_CASH_OUT_DATE", "<=",
						addFilter.getExhaustDateTo());
				
				querConstr.addSimpleExpression("out_of_dates", ") OR (", " ");
				
				
				querConstr.addElementIfNotNull("out_of_cashin_date", " ", "OUT_OF_CASH_IN_DATE", ">=",
						addFilter.getExhaustDateFrom());
				if(addFilter.getExhaustDateFrom() != null && addFilter.getExhaustDateTo() != null){
					querConstr.addSimpleExpression("out_of_dates", "AND", " ");
				}
				querConstr.addElementIfNotNull("out_of_cashin_date", " ", "OUT_OF_CASH_IN_DATE", "<=",
						addFilter.getExhaustDateTo());
				querConstr.addSimpleExpression("out_of_dates", "))", " ");
				
			}
			
			querConstr.setQueryTail(" ORDER BY EXTERNAL_ATM_ID");
			
			pstmt = con.prepareStatement(querConstr.getQuery());
			querConstr.updateQueryParameters(pstmt);
			rs = pstmt.executeQuery();
			while(rs.next()){
				AtmActualStateItem item = new AtmActualStateItem();
				item.setAtmID(rs.getInt("ATM_ID"));
				item.setExtAtmId(rs.getString("EXTERNAL_ATM_ID"));
				item.setCashInInit(rs.getInt("CASH_IN_BILLS_INITIAL"));
				item.setCashInLeft(rs.getInt("CASH_IN_BILLS_REMAINING"));
				item.setRejectInit(rs.getInt("REJECT_BILLS_INITIAL"));
				item.setRejectLeft(rs.getInt("REJECT_BILLS_REMAINING"));
				item.setDesc(CmUtils.getAtmFullAdrress(rs.getString("STATE"), rs.getString("CITY"), rs.getString("STREET")));
				
				item.setMainCurrCode(rs.getInt("MAIN_CURR_CODE"));
				item.setMainCurrA3(rs.getString("MAIN_CODE_A3"));
				item.setSec2CurrCode(rs.getInt("SECONDARY2_CURR_CODE"));
				item.setSec2CurrA3(rs.getString("SECONDARY2_CODE_A3"));
				item.setSec3CurrCode(rs.getInt("SECONDARY3_CURR_CODE"));
				item.setSec3CurrA3(rs.getString("SECONDARY3_CODE_A3"));
				item.setSecCurrCode(rs.getInt("SECONDARY_CURR_CODE"));
				item.setSecCurrA3(rs.getString("SECONDARY_CODE_A3"));
				
				item.setEncID(rs.getInt("CASH_OUT_ENC_ID"));
				item.setStatDate(rs.getTimestamp("CASH_OUT_STAT_DATE"));
				item.setStatLoadDate(rs.getTimestamp("STAT_LOAD_DATE"));
				
				item.setOutOfCashOutCurr(rs.getString("OUT_OF_CASH_OUT_CURR"));
				item.setOutOfCashOutDate(rs.getTimestamp("OUT_OF_CASH_OUT_DATE"));
				item.setOutOfCashOutResp(rs.getInt("OUT_OF_CASH_OUT_RESP"));
				
				item.setOutOfCashInDate(rs.getTimestamp("OUT_OF_CASH_IN_DATE"));
				item.setOutOfCashInResp(rs.getInt("OUT_OF_CASH_IN_RESP"));
				
				item.getRecRemainings().put(item.getMainCurrA3(), rs.getInt("MAIN_CURR_REC_REMAINING"));
				item.getRecRemainings().put(item.getSecCurrA3(), rs.getInt("SEC_CURR_REC_REMAINING"));
				item.getRecRemainings().put(item.getSec2CurrA3(), rs.getInt("SEC2_CURR_REC_REMAINING"));
				item.getRecRemainings().put(item.getSec3CurrA3(), rs.getInt("SEC3_CURR_REC_REMAINING"));
				

				item.getCoRemainings().put(item.getMainCurrA3(), rs.getInt("MAIN_CURR_REMAINING"));
				item.getCoRemainings().put(item.getSecCurrA3(), rs.getInt("SEC_CURR_REMAINING"));
				item.getCoRemainings().put(item.getSec2CurrA3(), rs.getInt("SEC2_CURR_REMAINING"));
				item.getCoRemainings().put(item.getSec3CurrA3(), rs.getInt("SEC3_CURR_REMAINING"));
				
				item.getAvgCashIn().put(item.getMainCurrA3(), rs.getInt("MAIN_CURR_CI"));
				item.getAvgCashIn().put(item.getSecCurrA3(), rs.getInt("SEC_CURR_CI"));
				item.getAvgCashIn().put(item.getSec2CurrA3(), rs.getInt("SEC2_CURR_CI"));
				item.getAvgCashIn().put(item.getSec3CurrA3(), rs.getInt("SEC3_CURR_CI"));
				
				if(rs.getInt("MAIN_CURR_CI_LAST_HOUR_DIFF") != 0){
					item.getAvgCashInDifference().put(item.getMainCurrA3(), Math.abs(rs.getInt("MAIN_CURR_CI_LAST_HOUR_DIFF")));
				}
				if(rs.getInt("SEC_CURR_CI_LAST_HOUR_DIFF") != 0){
					item.getAvgCashInDifference().put(item.getSecCurrA3(), Math.abs(rs.getInt("SEC_CURR_CI_LAST_HOUR_DIFF")));
				}
				if(rs.getInt("SEC2_CURR_CI_LAST_HOUR_DIFF") != 0){
					item.getAvgCashInDifference().put(item.getSec2CurrA3(), Math.abs(rs.getInt("SEC2_CURR_CI_LAST_HOUR_DIFF")));
				}
				if(rs.getInt("SEC3_CURR_CI_LAST_HOUR_DIFF") != 0){
					item.getAvgCashInDifference().put(item.getSec3CurrA3(), Math.abs(rs.getInt("SEC3_CURR_CI_LAST_HOUR_DIFF")));
				}
				
				item.getAvgCashInLastThreeHours().put(item.getMainCurrA3(), rs.getInt("MAIN_CURR_CI_LAST_THREE_HOURS"));
				item.getAvgCashInLastThreeHours().put(item.getSecCurrA3(), rs.getInt("SEC_CURR_CI_LAST_THREE_HOURS"));
				item.getAvgCashInLastThreeHours().put(item.getSec2CurrA3(), rs.getInt("SEC2_CURR_CI_LAST_THREE_HOURS"));
				item.getAvgCashInLastThreeHours().put(item.getSec3CurrA3(), rs.getInt("SEC3_CURR_CI_LAST_THREE_HOURS"));
				
				item.getAvgCashOut().put(item.getMainCurrA3(), rs.getInt("MAIN_CURR_CO"));
				item.getAvgCashOut().put(item.getSecCurrA3(), rs.getInt("SEC_CURR_CO"));
				item.getAvgCashOut().put(item.getSec2CurrA3(), rs.getInt("SEC2_CURR_CO"));
				item.getAvgCashOut().put(item.getSec3CurrA3(), rs.getInt("SEC3_CURR_CO"));
				
				if(rs.getInt("MAIN_CURR_CO_LAST_HOUR_DIFF") != 0){
					item.getAvgCashOutDifference().put(item.getMainCurrA3(), Math.abs(rs.getInt("MAIN_CURR_CO_LAST_HOUR_DIFF")));
				}
				if(rs.getInt("SEC_CURR_CO_LAST_HOUR_DIFF") != 0){
					item.getAvgCashOutDifference().put(item.getSecCurrA3(), Math.abs(rs.getInt("SEC_CURR_CO_LAST_HOUR_DIFF")));
				}
				if(rs.getInt("SEC2_CURR_CO_LAST_HOUR_DIFF") != 0){
					item.getAvgCashOutDifference().put(item.getSec2CurrA3(), Math.abs(rs.getInt("SEC2_CURR_CO_LAST_HOUR_DIFF")));
				}
				if(rs.getInt("SEC3_CURR_CO_LAST_HOUR_DIFF") != 0){
					item.getAvgCashOutDifference().put(item.getSec3CurrA3(), Math.abs(rs.getInt("SEC3_CURR_CO_LAST_HOUR_DIFF")));
				}
				
				item.getAvgCashOutLastThreeHours().put(item.getMainCurrA3(), rs.getInt("MAIN_CURR_CO_LAST_THREE_HOURS"));
				item.getAvgCashOutLastThreeHours().put(item.getSecCurrA3(), rs.getInt("SEC_CURR_CO_LAST_THREE_HOURS"));
				item.getAvgCashOutLastThreeHours().put(item.getSec2CurrA3(), rs.getInt("SEC2_CURR_CO_LAST_THREE_HOURS"));
				item.getAvgCashOutLastThreeHours().put(item.getSec3CurrA3(), rs.getInt("SEC3_CURR_CO_LAST_THREE_HOURS"));
				
				item.setLastWithdrHours(rs.getInt("LAST_WITHDRAWAL_HOURS"));
				item.setLastAddHours(rs.getInt("LAST_ADDITION_HOURS"));
				item.setAtmName(rs.getString("ATM_NAME"));
				item.setEmergencyEncashment(rs.getBoolean("EMERGENCY_ENCASHMENT"));
				item.setDateForthcomingEncashment(rs.getTimestamp("DATE_FORTHCOMING_ENCASHMENT"));
				
				item.setCashInEncId(rs.getInt("CASH_IN_ENC_ID"));
				item.setCashInRInit(rs.getInt("CASH_IN_R_BILLS_INITIAL"));
				
				item.getCurrs().put(rs.getString("MAIN_CURR_CODE"), item.getMainCurrA3());
				item.getCurrs().put(rs.getString("SECONDARY_CURR_CODE"), item.getSecCurrA3());
				item.getCurrs().put(rs.getString("SECONDARY2_CURR_CODE"), item.getSec2CurrA3());
				item.getCurrs().put(rs.getString("SECONDARY3_CURR_CODE"), item.getSec3CurrA3());
				
				item.setApproved(rs.getBoolean("IS_APPROVED"));
				item.setDaysUntilEncashment(rs.getInt("DAYS_UNTIL_ENCASHMENT"));
				item.setDaysUntilEncashment(rs.getInt("DAYS_UNTIL_ENCASHMENT"));
				item.setCashInClass(rs.getInt("CASH_IN_STATE") == 0 ? CASS_CI : CASS_NA);
				
				item.setCurrRemainingAlert(rs.getBoolean("CURR_REMAINING_ALERT"));
				
				atmActualStateList.add(item);
				
			}
			
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
			
			for(AtmActualStateItem item : atmActualStateList){
				String query =
					"select cass.cass_number, cass.cass_value,cs.cass_remaining,cs.cass_curr, en.CASS_COUNT as CASS_INIT, CI.CODE_A3,cass.CASS_STATE, bal.CASS_REMAINING_LOAD, bal.balance_status "+
					"from T_CM_ATM_CASSETTES cass "+
						"left outer join T_CM_CASHOUT_CASS_STAT cs on (" +
							"cs.atm_id = cass.atm_id " +
							"and cs.cass_number = cass.cass_number " +
							"and cs.cass_value = cass.cass_value " +
							"and cs.cass_curr = cass.cass_curr) " +
						"left outer join T_CM_INTGR_CASS_BALANCE bal on (" +
							"bal.atm_id = cass.atm_id " +
							"and bal.cass_number = cass.cass_number) " +
						"join (select sum(CASS_COUNT) as CASS_COUNT, ENCASHMENT_ID,CASS_NUMBER " +
							"from T_CM_ENC_CASHOUT_STAT_DETAILS where action_type in (2,4)" +
							"group by ENCASHMENT_ID,CASS_NUMBER) en on (en.encashment_id = cs.encashment_id and cs.cass_number = en.cass_number) " +
						"left outer join T_CM_CURR ci on (ci.code_n3 = cass.cass_curr) "+
					"where  cass.atm_id = ? "+
						"AND cass.CASS_TYPE = ? "+
						"AND cs.encashment_id = ? "+
						"AND cs.stat_date = ? " +
					"ORDER BY cs.CASS_NUMBER";
				pstmt = con.prepareStatement(query);
				pstmt.setInt(1, item.getAtmID());
				pstmt.setInt(2, AtmCassetteType.CASH_OUT_CASS.getId());
				pstmt.setInt(3, item.getEncID());
				pstmt.setTimestamp(4, new Timestamp(item.getStatDate().getTime()));
				rs = pstmt.executeQuery();
				while(rs.next()){
					AtmCashOutCassetteItem cass = new AtmCashOutCassetteItem(rs.getInt("CASS_NUMBER"), rs.getInt("CASS_VALUE"), rs.getInt("CASS_CURR"), rs.getBoolean("BALANCE_STATUS"));
					cass.setAmountInit(rs.getInt("CASS_INIT"));
					cass.setAmountLeft(rs.getInt("CASS_REMAINING"));
					cass.setAmountLeftFE(rs.getInt("CASS_REMAINING_LOAD"));
					cass.setCodeA3(rs.getString("CODE_A3"));
					cass.setPbClass(rs.getInt("CASS_STATE") == 0 ? rs.getString("CASS_CURR") : CASS_NA);
					item.getCashOutCassettes().add(cass);
				}
				JdbcUtils.close(pstmt);
				JdbcUtils.close(rs);
				
				query =
					"select cass.cass_number, cass.cass_value,csi.cass_remaining," +
							"sum(cs.CASS_COUNT_IN) as CASS_COUNT_IN,"+
							"sum(cs.CASS_COUNT_OUT) as CASS_COUNT_OUT,"+
							"cass.cass_curr, en.CASS_COUNT as CASS_INIT, CI.CODE_A3," +
							"cass.CASS_STATE, bal.CASS_REMAINING_LOAD, bal.balance_status "+
					"from T_CM_ATM_CASSETTES cass "+
						"left outer join (" +
							"select sum(CASS_COUNT) as CASS_COUNT, CASH_IN_ENCASHMENT_ID,CASS_NUMBER " +
							"from T_CM_ENC_CASHIN_STAT_DETAILS " +
							"where action_type in (2,4) and cash_in_encashment_id = ? " +
							"group by CASH_IN_ENCASHMENT_ID,CASS_NUMBER) en on (" +
							"cass.cass_number = en.cass_number) " +
						"left outer join T_CM_CASHIN_R_CASS_STAT cs on (" +
							"cs.atm_id = cass.atm_id " +
							"and cs.cass_number = cass.cass_number " +
							"and cs.cass_value = cass.cass_value " +
							"and cs.cass_curr = cass.cass_curr " +
							"AND cs.cash_in_encashment_id = ?) "+
						"left outer join T_CM_INTGR_CASS_BALANCE bal on (" +
							"bal.atm_id = cass.atm_id " +
							"and bal.cass_number = cass.cass_number) " +
						"left outer join T_CM_CURR ci on (ci.code_n3 = cass.cass_curr) " +
						"left outer join (select CASS_REMAINING,cass_number " +
								"from T_CM_CASHIN_R_CASS_STAT " +
								"where  atm_id = ? "+
									"AND cash_in_encashment_id = ? "+
									"AND stat_date = ? ) csi on (csi.cass_number = cass.cass_number) "+
					"where cass.atm_id = ? "+
						"AND cass.CASS_TYPE = ? "+
					"GROUP BY cass.cass_number, cass.cass_value,csi.cass_remaining,cass.cass_curr, CI.CODE_A3,cass.CASS_TYPE,cass.CASS_STATE,en.CASS_COUNT,bal.balance_status, bal.CASS_REMAINING_LOAD "+
					"ORDER BY cass.CASS_NUMBER";
				pstmt = con.prepareStatement(query);
				pstmt.setInt(1, item.getCashInEncId());
				pstmt.setInt(2, item.getCashInEncId());
				pstmt.setInt(3, item.getAtmID());
				pstmt.setInt(4, item.getCashInEncId());
				pstmt.setTimestamp(5, new Timestamp(item.getStatDate().getTime()));
				pstmt.setInt(6, item.getAtmID());
				pstmt.setInt(7, AtmCassetteType.CASH_IN_R_CASS.getId());
				rs = pstmt.executeQuery();
				while(rs.next()){
					AtmRecyclingCassetteItem cass = new AtmRecyclingCassetteItem(rs.getInt("CASS_NUMBER"), rs.getInt("CASS_VALUE"), rs.getInt("CASS_CURR"), rs.getBoolean("BALANCE_STATUS"));
					cass.setAmountInit(rs.getInt("CASS_INIT"));
					cass.setAmountIn(rs.getInt("CASS_COUNT_IN"));
					cass.setAmountOut(rs.getInt("CASS_COUNT_OUT"));
					cass.setAmountLeft(rs.getInt("CASS_REMAINING"));
					cass.setAmountLeftFE(rs.getInt("CASS_REMAINING_LOAD"));
					cass.setCodeA3(rs.getString("CODE_A3"));
					cass.setPbClass(rs.getInt("CASS_STATE") == 0 ? rs.getString("CASS_CURR") : CASS_NA);
					item.getCashInRCassettes().add(cass);
				}
				JdbcUtils.close(pstmt);
				JdbcUtils.close(rs);
				
				
				
			}
		} finally {
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}

		return atmActualStateList;
	}
	
	private static int checkDeltaCoeff(int lastHourDemand,int lastThreeHourDemand, double deltaLow, double deltaHigh){
		if(lastThreeHourDemand == 0){
			if(lastHourDemand > deltaLow){
				return lastHourDemand;
			}
		} else {
			double coeff = (double)lastHourDemand/(double)lastThreeHourDemand;
			
			if(coeff >= 0.35 && coeff <= 3.0){
				if(Math.abs(lastThreeHourDemand-lastHourDemand) > deltaHigh){
					return Math.abs(lastThreeHourDemand-lastHourDemand);
				}
			} else {
				if(Math.abs(lastThreeHourDemand-lastHourDemand) > deltaLow){
					return Math.abs(lastThreeHourDemand-lastHourDemand);
				}
			}
		}
		return 0;
	}
	
	protected static List<Integer> getCurrenciesList(Connection con,int atmId){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Integer> res = new ArrayList<Integer>();
		try {
			String query =
				"select MAIN_CURR_CODE, SECONDARY_CURR_CODE, SECONDARY2_CURR_CODE, SECONDARY3_CURR_CODE "+
				"from T_CM_ATM "+
				"where atm_id = ?";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, atmId);
			rs = pstmt.executeQuery();
			if(rs.next()){
				res.add(rs.getInt("MAIN_CURR_CODE"));
				res.add(rs.getInt("SECONDARY_CURR_CODE"));
				res.add(rs.getInt("SECONDARY2_CURR_CODE"));
				res.add(rs.getInt("SECONDARY3_CURR_CODE"));
			}
		} catch (Exception e) {
			logger.error("atmID = " + atmId,e);
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return res;
	}

	public static ObjectPair<Date,Integer> getCashOutLastStat(Connection con,int atmId){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ObjectPair<Date,Integer> res = null;
		try {
			String query =
				"select max(stat_date) as stat_date,max(encashment_id) as enc_id "+
				"from T_CM_CASHOUT_CASS_STAT "+
				"where atm_id = ?";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, atmId);
			rs = pstmt.executeQuery();
			if(rs.next()){
				res = new ObjectPair<Date,Integer>(CmUtils.getNVLValue(rs.getTimestamp("stat_date"),CmUtils.truncateDateToHours(new Date())),rs.getInt("enc_id"));
			}
		} catch (Exception e) {
			logger.error("atmID = " + atmId,e);
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return res;
	}
	
	protected static int getCashOutHoursFromLastWithdrawal(Connection con,int atmId){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int res=0;
		try {
			String query =
					"select t1.stat_date, t2.cass_count "+
					"from (select atm_id, max(stat_date) as stat_date "+
					"from T_CM_CASHOUT_CASS_STAT where cass_count<>0 group by atm_id) t1, T_CM_CASHOUT_CASS_STAT t2 "+
					"where t1.atm_id=t2.atm_id and t1.stat_date=t2.stat_date and t1.atm_id=?";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, atmId);
			rs = pstmt.executeQuery();
			if(rs.next()){
				res = CmUtils.getHoursBetweenTwoDates(rs.getTimestamp("stat_date"), new Date());
			}
		} catch (Exception e) {
			logger.error("atmID = " + atmId,e);
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return res;
	}
	
	protected static int getCashInHoursFromLastAddition(Connection con,int atmId){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int res=0;
		try {
			String query =
					"select t1.stat_date, t2.bills_count "+
					"from (select atm_id, max(stat_date) as stat_date "+
					"from T_CM_CASHIN_STAT where bills_count<>0 group by atm_id) t1, T_CM_CASHIN_STAT t2 "+
					"where t1.atm_id=t2.atm_id and t1.stat_date=t2.stat_date and t1.atm_id=?";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, atmId);
			rs = pstmt.executeQuery();
			if(rs.next()){
				res = CmUtils.getHoursBetweenTwoDates(rs.getTimestamp("stat_date"), new Date());
			}
		} catch (Exception e) {
			logger.error("atmID = " + atmId,e);
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return res;
	}
	
	public static ObjectPair<Date,Integer> getCashInLastStat(Connection con,int atmId){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ObjectPair<Date,Integer> res = null;
		try {
			String query =
				"select max(stat_date) as stat_date,max(cash_in_encashment_id) as enc_id "+
				"from T_CM_CASHIN_STAT "+
				"where atm_id = ?";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, atmId);
			rs = pstmt.executeQuery();
			if(rs.next()){
				res = new ObjectPair<Date,Integer>(rs.getTimestamp("stat_date"),rs.getInt("enc_id"));
			}
		} catch (Exception e) {
			logger.error("atmID = " + atmId,e);
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return res;
	}
	
	protected static double getAverageDemandStat(
			Map<Date, ForecastStatDay> days, Date startDate, Date endDate, int atmNotAvailableDays) {
		Date currentDate;
		double averageDemand = 0;
		double takeOffSumm = 0;
		int daysSize=7;
		//daysSize = days.size() == 0 ? 1 : days.size();
		for (Entry<Date, ForecastStatDay> entry : days.entrySet()) {
			currentDate=CmUtils.truncateDate(entry.getKey());
			if ((currentDate.compareTo(startDate)==0 || currentDate.after(startDate)) && (currentDate.compareTo(endDate)==0 || currentDate.before(endDate)))
					takeOffSumm += entry.getValue().getTakeOffs();
		}

		averageDemand = takeOffSumm / (daysSize - atmNotAvailableDays);
		
		return (averageDemand / (daysSize - atmNotAvailableDays));
	}

	//UPDATE LAST_UPDATE_DATE ONLY ON TASK CALL
	protected static void actualizeAtmStateForAtm(Connection con,AnyAtmForecast forecast, 
				ObjectPair<Date,Integer> cashOutStat,
				ObjectPair<Date,Integer> cashInStat, boolean updateLastUpdateDate){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		int check = 0;
		int paramIndex = 0;
		
		List<Currency> currencies = forecast.getAtmCurrencies();
		int currCount = currencies == null ? 0 : currencies.size();
		
		int mainCurrInDifference = 0;
		int mainCurrOutDifference = 0;
		int secCurrInDifference = 0;
		int secCurrOutDifference = 0;
		int sec2CurrInDifference = 0;
		int sec2CurrOutDifference = 0;
		int sec3CurrInDifference = 0;
		int sec3CurrOutDifference = 0;
		
		Currency mainCurrItem = currCount > 0 ? currencies.get(0) : null;
		Currency secCurrItem = currCount > 1 ? currencies.get(1) : null;
		Currency sec2CurrItem = currCount > 2 ? currencies.get(2) : null;
		Currency sec3CurrItem = currCount > 3 ? currencies.get(3) : null;
		CurrencyConverter converter = forecast.getCurrencyConverter();
		
		
		if(mainCurrItem != null){
			mainCurrInDifference = checkDeltaCoeff(
					mainCurrItem.getAvgStatRecInCurrLastHourDemand(), 
					mainCurrItem.getAvgStatRecInCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							mainCurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							mainCurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
			mainCurrOutDifference = checkDeltaCoeff(
					mainCurrItem.getAvgStatOutLastHourDemand()+mainCurrItem.getAvgStatRecOutCurrLastHourDemand(), 
					mainCurrItem.getAvgStatOutLastThreeHoursDemand()+mainCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							mainCurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							mainCurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
		}
		if(secCurrItem != null){
			secCurrInDifference = checkDeltaCoeff(
					secCurrItem.getAvgStatRecInCurrLastHourDemand(), 
					secCurrItem.getAvgStatRecInCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							secCurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							secCurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
			secCurrOutDifference = checkDeltaCoeff(
					secCurrItem.getAvgStatOutLastHourDemand()+secCurrItem.getAvgStatRecOutCurrLastHourDemand(), 
					secCurrItem.getAvgStatOutLastThreeHoursDemand()+secCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							secCurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							secCurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
		}
		if(sec2CurrItem != null){
			sec2CurrInDifference = checkDeltaCoeff(
					sec2CurrItem.getAvgStatRecInCurrLastHourDemand(), 
					sec2CurrItem.getAvgStatRecInCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec2CurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec2CurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
			sec2CurrOutDifference = checkDeltaCoeff(
					sec2CurrItem.getAvgStatOutLastHourDemand()+sec2CurrItem.getAvgStatRecOutCurrLastHourDemand(), 
					sec2CurrItem.getAvgStatOutLastThreeHoursDemand()+sec2CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec2CurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec2CurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
		}
		
		if(sec3CurrItem != null){
			sec3CurrInDifference = checkDeltaCoeff(
					sec3CurrItem.getAvgStatRecInCurrLastHourDemand(), 
					sec3CurrItem.getAvgStatRecInCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec3CurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec3CurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
			sec3CurrOutDifference = checkDeltaCoeff(
					sec3CurrItem.getAvgStatOutLastHourDemand()+sec3CurrItem.getAvgStatRecOutCurrLastHourDemand(), 
					sec3CurrItem.getAvgStatOutLastThreeHoursDemand()+sec3CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand(), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec3CurrItem.getCurrCode(), ALERT_DELTA_LOW), 
					converter.convertValue(ALERT_DELTA_CURR, 
							sec3CurrItem.getCurrCode(), ALERT_DELTA_HIGH)); 
		}
		
		
		try {
			
			String query =
				"SELECT count(1) as vcheck "+
			    "FROM T_CM_ATM_ACTUAL_STATE "+
			    "where atm_id = ? ";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, forecast.getAtmId());
			rs = pstmt.executeQuery();
			rs.next();
			check = rs.getInt("vcheck");

			JdbcUtils.close(rs);
			JdbcUtils.close(pstmt);

			
			if(check == 0){
				query =
					" INSERT INTO T_CM_ATM_ACTUAL_STATE "+
			        "(ATM_ID," +
			        "CASH_OUT_STAT_DATE,CASH_OUT_ENCASHMENT_ID, " +
			        "CASH_IN_STAT_DATE,CASH_IN_ENCASHMENT_ID, " +
			        "LAST_UPDATE," +
			        "OUT_OF_CASH_OUT_DATE,OUT_OF_CASH_OUT_CURR,OUT_OF_CASH_OUT_RESP," +
			        "OUT_OF_CASH_IN_DATE,OUT_OF_CASH_IN_RESP," +
			        "LAST_WITHDRAWAL_HOURS,LAST_ADDITION_HOURS," +
			        "CURR_REMAINING_ALERT) "+
			        "VALUES "+
			        "(?, " +
			        "?, ?, " +
			        "?, ?, " +
			        "?, " +
			        "?, ?, ?, " +
			        "?, ?, " +
			        "?, ?," +
			        "?)";
				pstmt = con.prepareStatement(query);
				pstmt.setInt(++paramIndex, forecast.getAtmId());
				pstmt.setTimestamp(++paramIndex, new Timestamp(cashOutStat.getKey().getTime()));
				pstmt.setInt(++paramIndex, cashOutStat.getValue());
				if(cashInStat != null && cashInStat.getKey() != null){
					pstmt.setTimestamp(++paramIndex, new Timestamp(cashInStat.getKey().getTime()));
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.DATE);
				}
				if(cashInStat != null && cashInStat.getValue() != null){
					pstmt.setInt(++paramIndex, cashInStat.getValue());
				} else {
					pstmt.setInt(++paramIndex, 0);
				}
				pstmt.setTimestamp(++paramIndex, new Timestamp(new Date().getTime()));
				
				if(forecast.getOutOfCashOutDate() != null){
					pstmt.setTimestamp(++paramIndex, 
							new Timestamp(CmUtils.truncateDateToHours(forecast.getOutOfCashOutDate()).getTime()));
					pstmt.setInt(++paramIndex, forecast.getOutOfCashOutCurr());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.DATE);
					pstmt.setNull(++paramIndex, java.sql.Types.INTEGER);
				}
				pstmt.setInt(++paramIndex, forecast.getOutOfCashOutResp());
				
				if(forecast.getOutOfCashInDate() != null){
					pstmt.setTimestamp(++paramIndex, 
							new Timestamp(CmUtils.truncateDateToHours(forecast.getOutOfCashInDate()).getTime()));
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.DATE);
				}
				pstmt.setInt(++paramIndex, forecast.getOutOfCashInResp());
				
								
				pstmt.setInt(++paramIndex, getCashOutHoursFromLastWithdrawal(con, forecast.getAtmId()));
				pstmt.setInt(++paramIndex, getCashInHoursFromLastAddition(con, forecast.getAtmId()));
				pstmt.setBoolean(++paramIndex, forecast.isNeedCurrRemainingAlert());
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
				
				query = 
				"INSERT INTO T_CM_ATM_AVG_DEMAND "+
					"(ATM_ID, "+
					"MAIN_CURR_CI, "+
					"MAIN_CURR_CO, "+
					"MAIN_CURR_CI_LAST_HOUR_DIFF, "+
					"MAIN_CURR_CO_LAST_HOUR_DIFF, "+
					"MAIN_CURR_CI_LAST_THREE_HOURS, "+
					"MAIN_CURR_CO_LAST_THREE_HOURS, "+
					"SEC_CURR_CI, "+
					"SEC_CURR_CO, "+
					"SEC_CURR_CI_LAST_HOUR_DIFF, "+
					"SEC_CURR_CO_LAST_HOUR_DIFF, "+
					"SEC_CURR_CI_LAST_THREE_HOURS, "+
					"SEC_CURR_CO_LAST_THREE_HOURS, "+
					"SEC2_CURR_CI, "+
					"SEC2_CURR_CO, "+
					"SEC2_CURR_CI_LAST_HOUR_DIFF, "+
					"SEC2_CURR_CO_LAST_HOUR_DIFF, "+
					"SEC2_CURR_CI_LAST_THREE_HOURS, "+
					"SEC2_CURR_CO_LAST_THREE_HOURS) " +
					"SEC3_CURR_CI, "+
					"SEC3_CURR_CO, "+
					"SEC3_CURR_CI_LAST_HOUR_DIFF, "+
					"SEC3_CURR_CO_LAST_HOUR_DIFF, "+
					"SEC3_CURR_CI_LAST_THREE_HOURS, "+
					"SEC3_CURR_CO_LAST_THREE_HOURS) " +
				"VALUES " +
					"(?," +
					"?, ?, ?, ?, ?, ?, " +
					"?, ?, ?, ?, ?, ?, " +
					"?, ?, ?, ?, ?, ?, " +
					"?, ?, ?, ?, ?, ?)";

				pstmt = con.prepareStatement(query);
				paramIndex = 0;

				pstmt.setInt(++paramIndex, forecast.getAtmId());
				if(mainCurrItem != null){
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatRecInCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatOutLastHourDemand()+mainCurrItem.getAvgStatRecOutCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, mainCurrInDifference);
					pstmt.setDouble(++paramIndex, mainCurrOutDifference);
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatOutLastThreeHoursDemand()+mainCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}
				if(secCurrItem != null){
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatRecInCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatOutLastHourDemand()+secCurrItem.getAvgStatRecOutCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, secCurrInDifference);
					pstmt.setDouble(++paramIndex, secCurrOutDifference);
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatOutLastThreeHoursDemand()+secCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}
				if(sec2CurrItem != null){
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatRecInCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatOutLastHourDemand()+sec2CurrItem.getAvgStatRecOutCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, sec2CurrInDifference);
					pstmt.setDouble(++paramIndex, sec2CurrOutDifference);
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatOutLastThreeHoursDemand()+sec2CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}
				if(sec3CurrItem != null){
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatRecInCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatOutLastHourDemand()+sec3CurrItem.getAvgStatRecOutCurrLastHourDemand());
					pstmt.setDouble(++paramIndex, sec3CurrInDifference);
					pstmt.setDouble(++paramIndex, sec3CurrOutDifference);
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatOutLastThreeHoursDemand()+sec3CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}

				
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
				
			} else {
				query =
					"UPDATE T_CM_ATM_ACTUAL_STATE "+
					"SET CASH_OUT_STAT_DATE = ?, "+
					"CASH_OUT_ENCASHMENT_ID = ?, "+
					"CASH_IN_STAT_DATE = ?, "+
					"CASH_IN_ENCASHMENT_ID = ?, "+
//					"LAST_UPDATE = ? ," +
					"OUT_OF_CASH_OUT_DATE = ?, " +
					"OUT_OF_CASH_OUT_CURR = ?, " +
					"OUT_OF_CASH_OUT_RESP = ?, " +
					"OUT_OF_CASH_IN_DATE = ?, " +
					"OUT_OF_CASH_IN_RESP = ?, " +
					"LAST_WITHDRAWAL_HOURS = ?, "+
					"LAST_ADDITION_HOURS = ?, " +
					"CURR_REMAINING_ALERT = ? "+
					"WHERE atm_id = ? ";
				pstmt = con.prepareStatement(query);
				
				pstmt.setTimestamp(++paramIndex, new Timestamp(cashOutStat.getKey().getTime()));
				pstmt.setInt(++paramIndex, cashOutStat.getValue());
				if(cashInStat != null && cashInStat.getKey() != null){
					pstmt.setTimestamp(++paramIndex, new Timestamp(cashInStat.getKey().getTime()));
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.DATE);
				}
				if(cashInStat != null && cashInStat.getValue() != null){
					pstmt.setInt(++paramIndex, cashInStat.getValue());
				} else {
					pstmt.setInt(++paramIndex, 0);
				}
				
				if(forecast.getOutOfCashOutDate() != null){
					pstmt.setTimestamp(++paramIndex, 
							new Timestamp(CmUtils.truncateDateToHours(forecast.getOutOfCashOutDate()).getTime()));
					pstmt.setInt(++paramIndex, forecast.getOutOfCashOutCurr());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.DATE);
					pstmt.setNull(++paramIndex, java.sql.Types.INTEGER);
				}
				pstmt.setInt(++paramIndex, forecast.getOutOfCashOutResp());
				
				if(forecast.getOutOfCashInDate() != null){
					pstmt.setTimestamp(++paramIndex, 
							new Timestamp(CmUtils.truncateDateToHours(forecast.getOutOfCashInDate()).getTime()));
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.DATE);
				}
				pstmt.setInt(++paramIndex, forecast.getOutOfCashInResp());
				
				pstmt.setInt(++paramIndex, getCashOutHoursFromLastWithdrawal(con, forecast.getAtmId()));
				pstmt.setInt(++paramIndex, getCashInHoursFromLastAddition(con, forecast.getAtmId()));
				
				pstmt.setBoolean(++paramIndex, forecast.isNeedCurrRemainingAlert());
				
				pstmt.setInt(++paramIndex, forecast.getAtmId());
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
				
				query = 
					"UPDATE T_CM_ATM_AVG_DEMAND "+
					"SET "+
						"MAIN_CURR_CI = ?, "+
						"MAIN_CURR_CO = ?, "+
						"MAIN_CURR_CI_LAST_HOUR_DIFF = ?, "+
						"MAIN_CURR_CO_LAST_HOUR_DIFF = ?, "+
						"MAIN_CURR_CI_LAST_THREE_HOURS = ?, "+
						"MAIN_CURR_CO_LAST_THREE_HOURS = ?, "+
						"SEC_CURR_CI = ?, "+
						"SEC_CURR_CO = ?, "+
						"SEC_CURR_CI_LAST_HOUR_DIFF = ?, "+
						"SEC_CURR_CO_LAST_HOUR_DIFF = ?, "+
						"SEC_CURR_CI_LAST_THREE_HOURS = ?, "+
						"SEC_CURR_CO_LAST_THREE_HOURS = ?, "+
						"SEC2_CURR_CI = ?, "+
						"SEC2_CURR_CO = ?, "+
						"SEC2_CURR_CI_LAST_HOUR_DIFF = ?, "+
						"SEC2_CURR_CO_LAST_HOUR_DIFF = ?, "+
						"SEC2_CURR_CI_LAST_THREE_HOURS = ?, "+
						"SEC2_CURR_CO_LAST_THREE_HOURS = ?, " +
						"SEC3_CURR_CI = ?, "+
						"SEC3_CURR_CO = ?, "+
						"SEC3_CURR_CI_LAST_HOUR_DIFF = ?, "+
						"SEC3_CURR_CO_LAST_HOUR_DIFF = ?, "+
						"SEC3_CURR_CI_LAST_THREE_HOURS = ?, "+
						"SEC3_CURR_CO_LAST_THREE_HOURS = ? " +
					"WHERE atm_id = ? ";

				pstmt = con.prepareStatement(query);
				paramIndex = 0;

				if(mainCurrItem != null){
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatRecInCurrDemand());
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatOutDemand()+mainCurrItem.getAvgStatRecOutCurrDemand());
					pstmt.setDouble(++paramIndex, mainCurrInDifference);
					pstmt.setDouble(++paramIndex, mainCurrOutDifference);
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, mainCurrItem.getAvgStatOutLastThreeHoursDemand()+mainCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}
				if(secCurrItem != null){
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatRecInCurrDemand());
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatOutDemand()+secCurrItem.getAvgStatRecOutCurrDemand());
					pstmt.setDouble(++paramIndex, secCurrInDifference);
					pstmt.setDouble(++paramIndex, secCurrOutDifference);
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, secCurrItem.getAvgStatOutLastThreeHoursDemand()+secCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}
				if(sec2CurrItem != null){
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatRecInCurrDemand());
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatOutDemand()+sec2CurrItem.getAvgStatRecOutCurrDemand());
					pstmt.setDouble(++paramIndex, sec2CurrInDifference);
					pstmt.setDouble(++paramIndex, sec2CurrOutDifference);
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, sec2CurrItem.getAvgStatOutLastThreeHoursDemand()+sec2CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}
				if(sec3CurrItem != null){
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatRecInCurrDemand());
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatOutDemand()+sec3CurrItem.getAvgStatRecOutCurrDemand());
					pstmt.setDouble(++paramIndex, sec3CurrInDifference);
					pstmt.setDouble(++paramIndex, sec3CurrOutDifference);
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					pstmt.setDouble(++paramIndex, sec3CurrItem.getAvgStatOutLastThreeHoursDemand()+sec3CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				} else {
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
					pstmt.setNull(++paramIndex, java.sql.Types.NUMERIC);
				}
				pstmt.setInt(++paramIndex, forecast.getAtmId());
				
				pstmt.executeUpdate();
				JdbcUtils.close(pstmt);
				
				if(updateLastUpdateDate){
					query =
						"UPDATE T_CM_ATM_ACTUAL_STATE "+
						"SET "+
						"LAST_UPDATE = ? " +
						"WHERE atm_id = ? ";
					pstmt = con.prepareStatement(query);
					pstmt.setTimestamp(1, new Timestamp(new Date().getTime()));
					pstmt.setInt(2, forecast.getAtmId());
					pstmt.executeUpdate();
					JdbcUtils.close(pstmt);
				}
			}
		} catch (SQLException e) {
			logger.error("atmID = " + forecast.getAtmId(),e);
		} finally{
			JdbcUtils.close(rs);
			JdbcUtils.close(pstmt);
		}
	}
	
	
	protected static void updateInitialsForAtm(Connection con,int atmId,
			int cashInVolume, int rejectVolume, int cashInRVolume){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try{
		
			String query =
				"UPDATE T_CM_ATM_ACTUAL_STATE "+
				"SET "+
					"CASH_IN_INITIAL = ?, " +
					"REJECT_INITIAL = ?, " +
					"CASH_IN_R_INITIAL = ? " +
				"WHERE atm_id = ? ";
			pstmt = con.prepareStatement(query);
			
			pstmt.setInt(1, cashInVolume);
			pstmt.setInt(2, rejectVolume);
			pstmt.setInt(3, cashInRVolume);
			pstmt.setInt(4, atmId);
			
			pstmt.executeUpdate();
			JdbcUtils.close(pstmt);
		
		} catch (SQLException e) {
			logger.error("atmID = " + atmId,e);
		} finally{
			JdbcUtils.close(rs);
			JdbcUtils.close(pstmt);
		}
	}


	public static boolean checkAtmActStateTable(Connection connection) {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int cnt = 0;
		try {
			String query =
				"SELECT count(1) as vcheck "+
			    "FROM T_CM_ATM_ACTUAL_STATE";
			pstmt = connection.prepareStatement(query);
			rs = pstmt.executeQuery();
			if(rs.next()){
				cnt = rs.getInt("VCHECK");
			}
		} catch (SQLException e){
			logger.error("",e);
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return cnt > 0;
	}
	
	protected static void addCashOutCassettes(Connection connection, int atmId, int ecnashmentId,List<AtmCassetteItem> cassList){
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			String query =
				"SELECT distinct CASS_NUMBER,CASS_VALUE,CASS_CURR "+
			    "FROM T_CM_CASHOUT_CASS_STAT " +
			    "WHERE encashment_id = ? and atm_id = ? ";
			pstmt = connection.prepareStatement(query);
			pstmt.setInt(1, ecnashmentId);
			pstmt.setInt(2, atmId);
			rs = pstmt.executeQuery();
			while(rs.next()){
				cassList.add(new AtmCassetteItem(rs.getInt("CASS_NUMBER"), rs.getInt("CASS_VALUE"), 
						rs.getInt("CASS_CURR"), AtmCassetteType.CASH_OUT_CASS, false));
			}
		} catch (SQLException e){
			logger.error("",e);
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
	}
	
	protected static void addCashInRecyclingCassettes(Connection connection, int atmId, int cashInEcnashmentId,
			List<AtmCassetteItem> cassList){
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			String query =
				"SELECT distinct CASS_NUMBER,CASS_VALUE,CASS_CURR "+
			    "FROM T_CM_CASHIN_R_CASS_STAT " +
			    "WHERE cash_in_encashment_id = ? and atm_id = ? ";
			pstmt = connection.prepareStatement(query);
			pstmt.setInt(1, cashInEcnashmentId);
			pstmt.setInt(2, atmId);
			rs = pstmt.executeQuery();
			while(rs.next()){
				cassList.add(new AtmCassetteItem(rs.getInt("CASS_NUMBER"), rs.getInt("CASS_VALUE"), 
						rs.getInt("CASS_CURR"), AtmCassetteType.CASH_IN_R_CASS, false));
			}
		} catch (SQLException e){
			logger.error("",e);
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		
	}

	public static void saveAtmCassettes(Connection con, int atmId,
			List<AtmCassetteItem> atmCassList) {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		int check = 0;

		try {
			
			String checkQuery =
				"SELECT count(1) as vcheck "+
			    "FROM T_CM_ATM_CASSETTES "+
			    "where " +
			    	"ATM_ID = ? " +
			    	"AND CASS_TYPE = ? " +
			    	"AND CASS_NUMBER = ? ";
			String insertQuery =
					" INSERT INTO T_CM_ATM_CASSETTES "+
			        "(ATM_ID, CASS_TYPE, CASS_NUMBER, CASS_CURR, CASS_VALUE) "+
			        "VALUES "+
			        "(?, ?, ?, ?, ?)";
			String updateQuery =
					"UPDATE T_CM_ATM_CASSETTES "+
			        "SET CASS_CURR = ? , " +
			        	"CASS_VALUE = ? "+
			        "where " +
			    	"ATM_ID = ? " +
			    	"AND CASS_TYPE = ? " +
			    	"AND CASS_NUMBER = ? ";
			String deleteQuery =
					"DELETE FROM T_CM_ATM_CASSETTES "+
			        "where " +
			    	"ATM_ID = ? " +
			    	"AND COALESCE(CASS_CURR,0) = 0 ";
			
			for(AtmCassetteItem cass : atmCassList){
				pstmt = con.prepareStatement(checkQuery);
				pstmt.setInt(1, atmId);
				pstmt.setInt(2, cass.getType().getId());
				pstmt.setInt(3, cass.getNumber());
				rs = pstmt.executeQuery();
				rs.next();
				check = rs.getInt("vcheck");
	
				JdbcUtils.close(rs);
				JdbcUtils.close(pstmt);
				
				
				if(check == 0){
					pstmt = con.prepareStatement(insertQuery);
					pstmt.setInt(1, atmId);
					pstmt.setInt(2, cass.getType().getId());
					pstmt.setInt(3, cass.getNumber());
					pstmt.setInt(4, cass.getCurr());
					pstmt.setInt(5, cass.getDenom());
					
					
					pstmt.executeUpdate();
					JdbcUtils.close(pstmt);
				} else {
					pstmt = con.prepareStatement(updateQuery);
					
					pstmt.setInt(1, cass.getCurr());
					pstmt.setInt(2, cass.getDenom());

					pstmt.setInt(3, atmId);
					pstmt.setInt(4, cass.getType().getId());
					pstmt.setInt(5, cass.getNumber());
			
					pstmt.executeUpdate();
					JdbcUtils.close(pstmt);
					
				}
			}
			
			pstmt = con.prepareStatement(deleteQuery);
			
			pstmt.setInt(1, atmId);
			pstmt.executeUpdate();
			
			JdbcUtils.close(pstmt);
		} catch (SQLException e) {
			logger.error("atmID = " + atmId,e);
		} finally{
			JdbcUtils.close(rs);
			JdbcUtils.close(pstmt);
		}
	}
	
	protected static void updateCalculatedRemainingForAtms(Connection con) throws SQLException{
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try{
		
			String query =
				"update t_cm_intgr_cass_balance intbal "+
				"SET CASS_REMAINING_CALC =  "+
				"(  "+
					"select cs.cass_remaining "+
					"from "+
						"t_cm_cashout_cass_stat cs "+
					"where  "+
						"cs.atm_id = intbal.atm_id "+
						"and "+
						"cs.cass_number = intbal.cass_number "+
						"and "+
						"cs.encashment_id = (select CASH_OUT_ENCASHMENT_ID from t_cm_atm_actual_state where atm_id = intbal.atm_id) "+
						"and "+
						"cs.stat_date = (select CASH_OUT_STAT_DATE from t_cm_atm_actual_state where atm_id = intbal.atm_id) "+
				") "+
				"WHERE "+
				" intbal.cass_type = ? ";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, AtmCassetteType.CASH_OUT_CASS.getId());
			pstmt.executeUpdate();
			JdbcUtils.close(pstmt);
			
			query =
				"update t_cm_intgr_cass_balance intbal "+
				"SET CASS_REMAINING_CALC =  "+
				"( "+
					"select cs.bills_remaining "+
					"from "+
					"t_cm_cashin_stat cs "+
					"where  "+
						"cs.atm_id = intbal.atm_id "+
						"and "+
						"cs.cash_in_encashment_id = (select CASH_IN_ENCASHMENT_ID from t_cm_atm_actual_state where atm_id = intbal.atm_id) "+
						"and "+
						"cs.stat_date = (select CASH_IN_STAT_DATE from t_cm_atm_actual_state where atm_id = intbal.atm_id) "+
				") "+
				"WHERE "+
					"intbal.cass_type = ? ";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, AtmCassetteType.CASH_IN_CASS.getId());
			pstmt.executeUpdate();
			JdbcUtils.close(pstmt);
			
			query =
				"update t_cm_intgr_cass_balance intbal "+
				"SET CASS_REMAINING_CALC =  "+
				"(  "+
					"select cs.cass_remaining "+
					"from "+
						"t_cm_cashin_r_cass_stat cs "+
					"where  "+
						"cs.atm_id = intbal.atm_id "+
						"and "+
						"cs.cass_number = intbal.cass_number "+
						"and "+
						"cs.cash_in_encashment_id = (select CASH_IN_ENCASHMENT_ID from t_cm_atm_actual_state where atm_id = intbal.atm_id) "+
						"and "+
						"cs.stat_date = (select CASH_IN_STAT_DATE from t_cm_atm_actual_state where atm_id = intbal.atm_id) "+
				") "+
				"WHERE "+
					"intbal.cass_type = ? ";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, AtmCassetteType.CASH_IN_R_CASS.getId());
			pstmt.executeUpdate();
			JdbcUtils.close(pstmt);
		
		} finally{
			JdbcUtils.close(rs);
			JdbcUtils.close(pstmt);
		}
	}

	public static void checkLoadedBalances(Connection connection) throws SQLException, MonitoringException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		boolean balancesAreIncorrect = false;
		
		try {
			String query =
				"select ATM_ID, CASS_TYPE, CASS_NUMBER, CASS_REMAINING_LOAD, CASS_REMAINING_CALC, BALANCE_STATUS "+
				"from t_cm_intgr_cass_balance "+
				"WHERE abs(COALESCE(CASS_REMAINING_LOAD,0) - COALESCE(CASS_REMAINING_CALC,0)) > ? " +
				" AND BALANCE_STATUS <> 1 " +
				"ORDER BY ATM_ID, CASS_TYPE, CASS_NUMBER";
			pstmt = connection.prepareStatement(query);
			pstmt.setInt(1, BALANCES_CHECK_THRESHHOLD);
			rs = pstmt.executeQuery();
			while(rs.next()){
				balancesAreIncorrect = true;
				logger.error(BALANCES_CHECK_ERROR_FORMAT, rs.getString("ATM_ID"), 
						rs.getString("CASS_TYPE"), rs.getString("CASS_NUMBER"), 
						rs.getString("CASS_REMAINING_LOAD"), rs.getString("CASS_REMAINING_CALC"));
			}
			if(balancesAreIncorrect){
				throw new MonitoringException("Loaded balances are incorrect, should be logged in CASH_MANAGEMENT logger");
			}
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		
	}

	public static void checkLoadedBalances(Connection connection, List<Integer> atmList) throws SQLException, MonitoringException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		boolean balancesAreIncorrect = false;
		
		try {
			StringBuffer query =
				new StringBuffer("select ATM_ID, CASS_TYPE, CASS_NUMBER, CASS_REMAINING_LOAD, CASS_REMAINING_CALC, BALANCE_STATUS "+
				"from t_cm_intgr_cass_balance "+
				"WHERE abs(COALESCE(CASS_REMAINING_LOAD,0) - COALESCE(CASS_REMAINING_CALC,0)) > ? " +
				" AND BALANCE_STATUS <> 1 and ");
			query.append(JdbcUtils.generateInConditionNumber("atm_id", atmList));
			query.append(" ORDER BY ATM_ID, CASS_TYPE, CASS_NUMBER");
			pstmt = connection.prepareStatement(query.toString());
			pstmt.setInt(1, BALANCES_CHECK_THRESHHOLD);
			rs = pstmt.executeQuery();
			while(rs.next()){
				balancesAreIncorrect = true;
				logger.error(BALANCES_CHECK_ERROR_FORMAT, rs.getString("ATM_ID"), 
						rs.getString("CASS_TYPE"), rs.getString("CASS_NUMBER"), 
						rs.getString("CASS_REMAINING_LOAD"), rs.getString("CASS_REMAINING_CALC"));
			}
			if(balancesAreIncorrect){
				throw new MonitoringException("Loaded balances are incorrect, should be logged in CASH_MANAGEMENT logger");
			}
		} finally{
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		
	}
	
	public static boolean getAtmDeviceState(Connection con, int atmId) {
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		try {
			String query = "SELECT ATM_STATE   "
					+ "FROM T_CM_ATM_ACTUAL_STATE " + "WHERE " + "ATM_ID = ? ";
			pstmt = con.prepareStatement(query);
			pstmt.setInt(1, atmId);
			rs = pstmt.executeQuery();
			if (rs.next()) {
				return rs.getInt("ATM_STATE") == 0;
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return false;
	}

}
