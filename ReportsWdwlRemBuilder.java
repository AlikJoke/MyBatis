package ru.bpc.cm.reports.orm.builders;

import java.util.Map;

import ru.bpc.cm.items.reports.ReportFilter;
import ru.bpc.cm.utils.TextUtil;
import ru.bpc.cm.utils.db.DbTextUtil;
import ru.bpc.cm.utils.db.QueryConstructor;

public class ReportsWdwlRemBuilder {

	public String getReportCoCurrWithdrawalRemainAtmBuilder(Map<String, Object> params) {
		ReportFilter repFilter = (ReportFilter) params.get("filter");
		String sql = "select ds.ATM_ID,ds.stat_date,ds.curr_code,ds.curr_summ,ds.curr_remaining,ci.code_a3, encashment_id "
				+ "from T_CM_CASHOUT_CURR_STAT ds join T_CM_CURR ci on (ds.curr_code = ci.code_n3) "
				+ "join T_CM_ATM ai on (ds.ATM_ID = ai.ATM_ID) where ds.STAT_DATE < #{dateTo} "
				+ "AND ds.atm_id in (select id from t_cm_temp_atm_list) AND ds.STAT_DATE > #{dateFrom} "
				+ "AND (ds.CURR_CODE = #{wdwlRemCurrCode} OR #{wdwlRemCurrCode} = 0) ";
		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql.toString(), true);
		querConstr.addElementIfNotNull("atm_id", "AND", "ds.ATM_ID", DbTextUtil.getOperationType(repFilter.getAtmId()),
				repFilter.getAtmId());
		if (TextUtil.isNotEmpty(repFilter.getNameAndAddress())) {
			querConstr.addElement("address", "AND ( ", "ai.state || ', ' || ai.city || ', ' || ai.street",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addElement("atm_name", "OR", "ai.NAME",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addSimpleExpression("atm_name_addr", ")", " ");
		}
		querConstr.setQueryTail("order by ATM_ID,curr_code,stat_date, encashment_id ");
		String query = querConstr.toString();
		if (repFilter.getAtmId() != null) {
			query.replaceFirst("//?", "#{atmId}");
		}
		if (repFilter.getNameAndAddress() != null) {
			query.replaceAll("//?", "#{nameAndAddr}");
		}
		return query;
	}

	public String getReportCoCurrWithdrawalRemainGroupBuilder(Map<String, Object> params) {
		ReportFilter repFilter = (ReportFilter) params.get("filter");
		String sql = "SELECT ds.stat_date, ds.curr_code, sum(ds.curr_summ) as CURR_SUMM, "
				+ "sum(CURR_REMAINING) as CURR_REMAINING, ci.code_a3 FROM ( SELECT "
				+ "ds.ATM_ID,ds.STAT_DATE,ds.CURR_CODE, sum(ds.CURR_SUMM) as CURR_SUMM, ds.CURR_REMAINING "
				+ "FROM ( SELECT ds.ATM_ID,ds.STAT_DATE,ds.CURR_CODE,ds.CURR_SUMM as CURR_SUMM, "
				+ "LAST_VALUE(ds.curr_remaining) over "
				+ "(partition by ds.ATM_ID,ds.CURR_CODE,ds.STAT_DATE ORDER BY ds.stat_date) as CURR_REMAINING "
				+ "FROM T_CM_CASHOUT_CURR_STAT ds join T_CM_ATM ai on (ds.ATM_ID = ai.ATM_ID) "
				+ "WHERE ds.STAT_DATE < #{dateTo} AND ds.atm_id in (select id from t_cm_temp_atm_list) "
				+ "AND ds.STAT_DATE > #{dateFrom} AND (ds.CURR_CODE = #{wdwlRemCurrCode} OR #{wdwlRemCurrCode} = 0) ";

		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql.toString(), true);
		querConstr.addElementIfNotNull("atm_id", "AND", "ds.ATM_ID", DbTextUtil.getOperationType(repFilter.getAtmId()),
				repFilter.getAtmId());
		if (TextUtil.isNotEmpty(repFilter.getNameAndAddress())) {
			querConstr.addElement("address", "AND ( ", "ai.state || ', ' || ai.city || ', ' || ai.street",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addElement("atm_name", "OR", "ai.NAME",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addSimpleExpression("atm_name_addr", ")", " ");
		}
		querConstr.setQueryTail(") ds GROUP BY ds.ATM_ID,ds.STAT_DATE,ds.curr_remaining,ds.CURR_CODE ) ds "
				+ "join T_CM_CURR ci on (ds.curr_code = ci.code_n3) GROUP BY ds.stat_date,ds.curr_code,ci.code_a3 "
				+ "order by curr_Code,stat_date ");
		String query = querConstr.toString();
		if (repFilter.getAtmId() != null) {
			query.replaceFirst("//?", "#{atmId}");
		}
		if (repFilter.getNameAndAddress() != null) {
			query.replaceAll("//?", "#{nameAndAddr}");
		}
		return query;
	}

	public String getReportCrCurrWithdrawalRemainAtmBuilder(Map<String, Object> params) {
		ReportFilter repFilter = (ReportFilter) params.get("filter");
		String sql = "select ds.ATM_ID,ds.stat_date,ds.curr_code, "
				+ "ds.curr_summ_in, ds.curr_summ_out, ds.curr_remaining, "
				+ "ci.code_a3, cash_in_encashment_id as encashment_id from T_CM_CASHIN_R_CURR_STAT ds "
				+ "join T_CM_CURR ci on (ds.curr_code = ci.code_n3) join T_CM_ATM ai on (ds.ATM_ID = ai.ATM_ID) "
				+ "where ds.STAT_DATE < #{dateTo} AND ds.atm_id in (select id from t_cm_temp_atm_list) "
				+ "AND ds.STAT_DATE > #{dateFrom} "
				+ "AND (ds.CURR_CODE = #{wdwlRemCurrCode} OR #{wdwlRemCurrCode} = 0) ";
		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql.toString(), true);
		querConstr.addElementIfNotNull("atm_id", "AND", "ds.ATM_ID", DbTextUtil.getOperationType(repFilter.getAtmId()),
				repFilter.getAtmId());
		if (TextUtil.isNotEmpty(repFilter.getNameAndAddress())) {
			querConstr.addElement("address", "AND ( ", "ai.state || ', ' || ai.city || ', ' || ai.street",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addElement("atm_name", "OR", "ai.NAME",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addSimpleExpression("atm_name_addr", ")", " ");
		}
		querConstr.setQueryTail("order by ATM_ID,curr_code,stat_date,cash_in_encashment_id ");
		String query = querConstr.toString();
		if (repFilter.getAtmId() != null) {
			query.replaceFirst("//?", "#{atmId}");
		}
		if (repFilter.getNameAndAddress() != null) {
			query.replaceAll("//?", "#{nameAndAddr}");
		}
		return query;
	}

	public String getReportCrCurrWithdrawalRemainGroupBuilder(Map<String, Object> params) {
		ReportFilter repFilter = (ReportFilter) params.get("filter");
		String sql = "SELECT ds.stat_date, ds.curr_code, sum(ds.CURR_SUMM_IN) as CURR_SUMM_IN, "
				+ "sum(ds.CURR_SUMM_OUT) as CURR_SUMM_OUT, sum(CURR_REMAINING) as CURR_REMAINING, ci.code_a3 "
				+ "FROM ( SELECT ds.ATM_ID,ds.STAT_DATE,ds.CURR_CODE, "
				+ "sum(ds.CURR_SUMM_IN) as CURR_SUMM_IN, sum(ds.CURR_SUMM_OUT) as CURR_SUMM_OUT, "
				+ "ds.CURR_REMAINING FROM ( SELECT ds.ATM_ID,ds.STAT_DATE,ds.CURR_CODE,"
				+ "ds.CURR_SUMM_IN as CURR_SUMM_IN, ds.CURR_SUMM_OUT as CURR_SUMM_OUT, "
				+ "LAST_VALUE(ds.curr_remaining) over "
				+ "(partition by ds.ATM_ID,ds.CURR_CODE,ds.STAT_DATE ORDER BY ds.stat_date) as CURR_REMAINING "
				+ "FROM T_CM_CASHIN_R_CURR_STAT ds join T_CM_ATM ai on (ds.ATM_ID = ai.ATM_ID) "
				+ "WHERE ds.STAT_DATE < #{dateTo} AND ds.atm_id in (select id from t_cm_temp_atm_list) "
				+ "AND ds.STAT_DATE > #{dateFrom} AND (ds.CURR_CODE = #{wdwlRemCurrCode} OR #{wdwlRemCurrCode} = 0) ";
		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql.toString(), true);
		querConstr.addElementIfNotNull("atm_id", "AND", "ds.ATM_ID", DbTextUtil.getOperationType(repFilter.getAtmId()),
				repFilter.getAtmId());
		if (TextUtil.isNotEmpty(repFilter.getNameAndAddress())) {
			querConstr.addElement("address", "AND ( ", "ai.state || ', ' || ai.city || ', ' || ai.street",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addElement("atm_name", "OR", "ai.NAME",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addSimpleExpression("atm_name_addr", ")", " ");
		}
		querConstr.setQueryTail(") ds GROUP BY ds.ATM_ID,ds.STAT_DATE,ds.curr_remaining,ds.CURR_CODE ) ds "
				+ "join T_CM_CURR ci on (ds.curr_code = ci.code_n3) GROUP BY ds.stat_date,ds.curr_code,ci.code_a3 "
				+ "order by curr_Code,stat_date ");
		String query = querConstr.toString();
		if (repFilter.getAtmId() != null) {
			query.replaceFirst("//?", "#{atmId}");
		}
		if (repFilter.getNameAndAddress() != null) {
			query.replaceAll("//?", "#{nameAndAddr}");
		}
		return query;
	}

	public String getReportCiBillWithdrawalRemainAtmBuilder(Map<String, Object> params) {
		ReportFilter repFilter = (ReportFilter) params.get("filter");
		String sql = "select ds.ATM_ID, ds.stat_date, ds.BILLS_COUNT as curr_summ, "
				+ "ds.BILLS_REMAINING as curr_remaining, cash_in_encashment_id as encashment_id "
				+ "from T_CM_CASHIN_STAT ds join T_CM_ATM ai on (ds.ATM_ID = ai.ATM_ID) "
				+ "where ds.STAT_DATE < #{dateTo} AND ds.atm_id in (select id from t_cm_temp_atm_list) "
				+ "AND ds.STAT_DATE > #{dateFrom} ";
		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql.toString(), true);
		querConstr.addElementIfNotNull("atm_id", "AND", "ds.ATM_ID", DbTextUtil.getOperationType(repFilter.getAtmId()),
				repFilter.getAtmId());
		if (TextUtil.isNotEmpty(repFilter.getNameAndAddress())) {
			querConstr.addElement("address", "AND ( ", "ai.state || ', ' || ai.city || ', ' || ai.street",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addElement("atm_name", "OR", "ai.NAME",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addSimpleExpression("atm_name_addr", ")", " ");
		}
		querConstr.setQueryTail("order by ATM_ID,stat_date, cash_in_encashment_id ");
		String query = querConstr.toString();
		if (repFilter.getAtmId() != null) {
			query.replaceFirst("//?", "#{atmId}");
		}
		if (repFilter.getNameAndAddress() != null) {
			query.replaceAll("//?", "#{nameAndAddr}");
		}
		return query;
	}

	public String getReportCiBillWithdrawalRemainGroupBuilder(Map<String, Object> params) {
		ReportFilter repFilter = (ReportFilter) params.get("filter");
		String sql = "SELECT ds.stat_date, sum(ds.curr_summ) as CURR_SUMM, "
				+ "sum(CURR_REMAINING) as CURR_REMAINING FROM ( SELECT ds.ATM_ID,ds.STAT_DATE, "
				+ "sum(ds.CURR_SUMM) as CURR_SUMM, ds.CURR_REMAINING FROM ( "
				+ "SELECT ds.ATM_ID,ds.STAT_DATE,ds.BILLS_COUNT as CURR_SUMM, LAST_VALUE(ds.BILLS_REMAINING) over "
				+ "(partition by ds.ATM_ID,ds.STAT_DATE ORDER BY ds.stat_date) as CURR_REMAINING "
				+ "FROM T_CM_CASHIN_STAT ds join T_CM_ATM ai on (ds.ATM_ID = ai.ATM_ID) "
				+ "WHERE ds.STAT_DATE < {dateTo} AND ds.atm_id in (select id from t_cm_temp_atm_list) "
				+ "AND ds.STAT_DATE > #{dateFrom} ";
		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql.toString(), true);
		querConstr.addElementIfNotNull("atm_id", "AND", "ds.ATM_ID", DbTextUtil.getOperationType(repFilter.getAtmId()),
				repFilter.getAtmId());
		if (TextUtil.isNotEmpty(repFilter.getNameAndAddress())) {
			querConstr.addElement("address", "AND ( ", "ai.state || ', ' || ai.city || ', ' || ai.street",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addElement("atm_name", "OR", "ai.NAME",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addSimpleExpression("atm_name_addr", ")", " ");
		}
		querConstr.setQueryTail(") ds GROUP BY ds.ATM_ID,ds.STAT_DATE,ds.curr_remaining ) ds "
				+ "GROUP BY ds.stat_date order by stat_date ");
		String query = querConstr.toString();
		if (repFilter.getAtmId() != null) {
			query.replaceFirst("//?", "#{atmId}");
		}
		if (repFilter.getNameAndAddress() != null) {
			query.replaceAll("//?", "#{nameAndAddr}");
		}
		return query;
	}

	public String getReportCiCurrWithdrawalRemainAtBuilder(Map<String, Object> params) {
		ReportFilter repFilter = (ReportFilter) params.get("filter");
		String sql = "select ds.ATM_ID,ds.stat_date,ds.cash_in_encashment_id,ds.CURR_SUMM, "
				+ "sum(ds.CURR_SUMM) over (partition by ds.ATM_ID,ds.cash_in_encashment_id,ds.denom_curr "
				+ "order by ds.stat_date,ds.cash_in_encashment_id) as CURR_REMAINING, "
				+ "ds.denom_curr, ds.code_a3, ds.cash_in_encashment_id as encashment_id from( "
				+ "select ds.ATM_ID,ds.stat_date,sum(ds.denom_value*ds.denom_count) as CURR_SUMM,"
				+ "ds.cash_in_encashment_id,ds.denom_curr,ci.code_a3 from T_CM_CASHIN_DENOM_STAT ds "
				+ "join T_CM_CURR ci on (ds.denom_curr = ci.code_n3) join T_CM_ATM ai on (ds.ATM_ID = ai.ATM_ID) "
				+ "where ds.STAT_DATE <= #{dateTo} AND ds.atm_id in (select id from t_cm_temp_atm_list) "
				+ "AND ds.STAT_DATE >= #{dateFrom} "
				+ "AND (ds.denom_curr = #{wdwlRemCurrCode} OR #{wdwlRemCurrCode} = 0) ";
		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql.toString(), true);
		querConstr.addElementIfNotNull("atm_id", "AND", "ds.ATM_ID", DbTextUtil.getOperationType(repFilter.getAtmId()),
				repFilter.getAtmId());
		if (TextUtil.isNotEmpty(repFilter.getNameAndAddress())) {
			querConstr.addElement("address", "AND ( ", "ai.state || ', ' || ai.city || ', ' || ai.street",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addElement("atm_name", "OR", "ai.NAME",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addSimpleExpression("atm_name_addr", ")", " ");
		}
		querConstr.setQueryTail(" group by ds.ATM_ID,ds.stat_date,ds.cash_in_encashment_id,ds.denom_curr, ci.code_a3 "
				+ ") ds order by ds.ATM_ID,DENOM_CURR,STAT_DATE, ds.cash_in_encashment_id ");
		String query = querConstr.toString();
		if (repFilter.getAtmId() != null) {
			query.replaceFirst("//?", "#{atmId}");
		}
		if (repFilter.getNameAndAddress() != null) {
			query.replaceAll("//?", "#{nameAndAddr}");
		}
		return query;
	}

	public String getReportCoDenomWithdrawalRemainAtmBuilder(Map<String, Object> params) {
		ReportFilter repFilter = (ReportFilter) params.get("filter");
		String sql = "select ds.ATM_ID,ds.stat_date,ds.denom_value, ds.denom_curr,ds.denom_count as DENOM_COUNT, "
				+ "ds.denom_remaining as DENOM_REMAINING,ci.code_a3, encashment_id "
				+ "from V_CM_CASHOUT_DENOM_STAT ds join T_CM_CURR ci on (ds.denom_curr = ci.code_n3) "
				+ "join T_CM_ATM ai on (ds.ATM_ID = ai.ATM_ID) where ds.STAT_DATE <= #{dateTo} "
				+ "AND ds.atm_id in (select id from t_cm_temp_atm_list) AND ds.STAT_DATE >= #{} "
				+ "AND (ds.denom_curr = #{wdwlRemCurrCode} OR #{wdwlRemCurrCode} = 0) ";
		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql.toString(), true);
		querConstr.addElementIfNotNull("atm_id", "AND", "ds.ATM_ID", DbTextUtil.getOperationType(repFilter.getAtmId()),
				repFilter.getAtmId());
		if (TextUtil.isNotEmpty(repFilter.getNameAndAddress())) {
			querConstr.addElement("address", "AND ( ", "ai.state || ', ' || ai.city || ', ' || ai.street",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addElement("atm_name", "OR", "ai.NAME",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addSimpleExpression("atm_name_addr", ")", " ");
		}
		querConstr.setQueryTail(" order by ds.ATM_ID,DENOM_CURR,DENOM_VALUE desc,STAT_DATE,encashment_id ");
		String query = querConstr.toString();
		if (repFilter.getAtmId() != null) {
			query.replaceFirst("//?", "#{atmId}");
		}
		if (repFilter.getNameAndAddress() != null) {
			query.replaceAll("//?", "#{nameAndAddr}");
		}
		return query;
	}

	public String getReportCiCurrWithdrawalRemainGroupBuilder(Map<String, Object> params) {
		ReportFilter repFilter = (ReportFilter) params.get("filter");
		String sql = "SELECT ds.stat_date,ds.denom_curr as denom_curr,"
				+ "sum(ds.CURR_SUMM) as CURR_SUMM,sum(ds.CURR_REMAINING) as CURR_REMAINING,ds.code_a3 FROM ("
				+ "select ds.ATM_ID,ds.stat_date,ds.cash_in_encashment_id,ds.CURR_SUMM, "
				+ "sum(ds.CURR_SUMM) over (partition by ds.ATM_ID,ds.cash_in_encashment_id,ds.denom_curr "
				+ "order by ds.stat_date,ds.cash_in_encashment_id) as CURR_REMAINING, ds.denom_curr,ds.code_a3 "
				+ "from( select ds.ATM_ID,ds.stat_date,sum(ds.denom_value*ds.denom_count) as CURR_SUMM,"
				+ "ds.cash_in_encashment_id,ds.denom_curr,ci.code_a3 from T_CM_CASHIN_DENOM_STAT ds "
				+ "join T_CM_CURR ci on (ds.denom_curr = ci.code_n3) join T_CM_ATM ai on (ds.ATM_ID = ai.ATM_ID) "
				+ "where ds.STAT_DATE <= #{dateTo} AND ds.atm_id in (select id from t_cm_temp_atm_list) "
				+ "AND ds.STAT_DATE >= #{dateFrom} AND (ds.denom_curr = #{wdwlRemCurrCode} OR #{wdwlRemCurrCode} = 0) ";
		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql.toString(), true);
		querConstr.addElementIfNotNull("atm_id", "AND", "ds.ATM_ID", DbTextUtil.getOperationType(repFilter.getAtmId()),
				repFilter.getAtmId());
		if (TextUtil.isNotEmpty(repFilter.getNameAndAddress())) {
			querConstr.addElement("address", "AND ( ", "ai.state || ', ' || ai.city || ', ' || ai.street",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addElement("atm_name", "OR", "ai.NAME",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addSimpleExpression("atm_name_addr", ")", " ");
		}
		querConstr.setQueryTail("group by ds.ATM_ID,ds.stat_date,ds.cash_in_encashment_id,ds.denom_curr, ci.code_a3 "
				+ ") ds ) ds group by ds.stat_date,ds.denom_curr,ds.code_a3 order by DENOM_CURR,STAT_DATE ");
		String query = querConstr.toString();
		if (repFilter.getAtmId() != null) {
			query.replaceFirst("//?", "#{atmId}");
		}
		if (repFilter.getNameAndAddress() != null) {
			query.replaceAll("//?", "#{nameAndAddr}");
		}
		return query;
	}

	public String getReportCoDenomWithdrawalRemainGroupBuilder(Map<String, Object> params) {
		ReportFilter repFilter = (ReportFilter) params.get("filter");
		String sql = "SELECT ds.stat_date, ds.curr_code,ds.denom_value, sum(ds.curr_summ) as CURR_SUMM, "
				+ "sum(CURR_REMAINING) as CURR_REMAINING, ci.code_a3 FROM ( SELECT "
				+ "ds.ATM_ID, ds.STAT_DATE,ds.CURR_CODE,ds.denom_value, sum(ds.CURR_SUMM) as CURR_SUMM, "
				+ "ds.CURR_REMAINING FROM ( "
				+ "SELECT ds.ATM_ID, ds.STAT_DATE, ds.denom_curr as CURR_CODE,ds.denom_value,ds.denom_count as CURR_SUMM, "
				+ "LAST_VALUE(ds.denom_remaining) over "
				+ "(partition by ds.ATM_ID,ds.denom_curr,ds.STAT_DATE ORDER BY ds.stat_date) as CURR_REMAINING "
				+ "FROM V_CM_CASHOUT_DENOM_STAT ds  join T_CM_ATM ai on (ds.ATM_ID = ai.ATM_ID) "
				+ "WHERE ds.STAT_DATE < #{dateTo} AND ds.atm_id in (select id from t_cm_temp_atm_list) "
				+ "AND ds.STAT_DATE > #{dateFrom} AND (ds.denom_curr = #{wdwlRemCurrCode} OR #{wdwlRemCurrCode} = 0) ";
		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql.toString(), true);
		querConstr.addElementIfNotNull("atm_id", "AND", "ds.ATM_ID", DbTextUtil.getOperationType(repFilter.getAtmId()),
				repFilter.getAtmId());
		if (TextUtil.isNotEmpty(repFilter.getNameAndAddress())) {
			querConstr.addElement("address", "AND ( ", "ai.state || ', ' || ai.city || ', ' || ai.street",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addElement("atm_name", "OR", "ai.NAME",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addSimpleExpression("atm_name_addr", ")", " ");
		}
		querConstr.setQueryTail(") ds GROUP BY ds.ATM_ID,ds.STAT_DATE,ds.curr_remaining,ds.curr_code,ds.denom_value "
				+ ") ds join T_CM_CURR ci on (ds.curr_code = ci.code_n3) "
				+ "GROUP BY ds.stat_date,ds.curr_code,ds.denom_value,ci.code_a3 "
				+ "order by curr_code,denom_value,stat_date ");
		String query = querConstr.toString();
		if (repFilter.getAtmId() != null) {
			query.replaceFirst("//?", "#{atmId}");
		}
		if (repFilter.getNameAndAddress() != null) {
			query.replaceAll("//?", "#{nameAndAddr}");
		}
		return query;
	}

	public String getReportCrDenomWithdrawalRemainAtmBuilder(Map<String, Object> params) {
		ReportFilter repFilter = (ReportFilter) params.get("filter");
		String sql = "select ds.ATM_ID,ds.stat_date,ds.denom_value,ds.denom_curr,"
				+ "ds.denom_count_in as DENOM_COUNT_IN,ds.denom_count_out as DENOM_COUNT_OUT,"
				+ "ds.denom_remaining as DENOM_REMAINING,ci.code_a3, cash_in_encashment_id as encashment_id "
				+ "from V_CM_CASHIN_R_DENOM_STAT ds join T_CM_CURR ci on (ds.denom_curr = ci.code_n3) "
				+ " join T_CM_ATM ai on (ds.ATM_ID = ai.ATM_ID) where ds.STAT_DATE <= #{dateTo} "
				+ "AND ds.atm_id in (select id from t_cm_temp_atm_list) AND ds.STAT_DATE >= #{dateFrom} "
				+ "AND (ds.denom_curr = #{wdwlRemCurrCode} OR #{wdwlRemCurrCode} = 0) ";
		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql.toString(), true);
		querConstr.addElementIfNotNull("atm_id", "AND", "ds.ATM_ID", DbTextUtil.getOperationType(repFilter.getAtmId()),
				repFilter.getAtmId());
		if (TextUtil.isNotEmpty(repFilter.getNameAndAddress())) {
			querConstr.addElement("address", "AND ( ", "ai.state || ', ' || ai.city || ', ' || ai.street",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addElement("atm_name", "OR", "ai.NAME",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addSimpleExpression("atm_name_addr", ")", " ");
		}
		querConstr.setQueryTail("order by ds.ATM_ID,DENOM_CURR,DENOM_VALUE desc,STAT_DATE,cash_in_encashment_id ");
		String query = querConstr.toString();
		if (repFilter.getAtmId() != null) {
			query.replaceFirst("//?", "#{atmId}");
		}
		if (repFilter.getNameAndAddress() != null) {
			query.replaceAll("//?", "#{nameAndAddr}");
		}
		return query;
	}

	public String getReportCrDenomWithdrawalRemainGroupBuilder(Map<String, Object> params) {
		ReportFilter repFilter = (ReportFilter) params.get("filter");
		String sql = "SELECT ds.stat_date, ds.curr_code,ds.denom_value, "
				+ "sum(ds.CURR_SUMM_IN) as CURR_SUMM_IN, sum(ds.CURR_SUMM_OUT) as CURR_SUMM_OUT, "
				+ "sum(CURR_REMAINING) as CURR_REMAINING, ci.code_a3 FROM ( SELECT "
				+ "ds.ATM_ID,ds.STAT_DATE,ds.CURR_CODE,ds.denom_value, sum(ds.CURR_SUMM_IN) as CURR_SUMM_IN, "
				+ "sum(ds.CURR_SUMM_OUT) as CURR_SUMM_OUT, ds.CURR_REMAINING FROM ( "
				+ "SELECT ds.ATM_ID,ds.STAT_DATE,ds.denom_curr as CURR_CODE,ds.denom_value,"
				+ "ds.denom_count_in as CURR_SUMM_IN,ds.denom_count_out as CURR_SUMM_OUT,"
				+ "LAST_VALUE(ds.denom_remaining) over "
				+ "(partition by ds.ATM_ID,ds.denom_value,ds.STAT_DATE ORDER BY ds.stat_date) as CURR_REMAINING "
				+ "FROM V_CM_CASHIN_R_DENOM_STAT ds  join T_CM_ATM ai on (ds.ATM_ID = ai.ATM_ID) "
				+ "WHERE ds.STAT_DATE < #{dateTo} AND ds.atm_id in (select id from t_cm_temp_atm_list) "
				+ "AND ds.STAT_DATE > #{dateFrom} AND (ds.denom_curr = #{wdwlRemCurrCode} OR #{wdwlRemCurrCode} = 0) ";

		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql.toString(), true);
		querConstr.addElementIfNotNull("atm_id", "AND", "ds.ATM_ID", DbTextUtil.getOperationType(repFilter.getAtmId()),
				repFilter.getAtmId());
		if (TextUtil.isNotEmpty(repFilter.getNameAndAddress())) {
			querConstr.addElement("address", "AND ( ", "ai.state || ', ' || ai.city || ', ' || ai.street",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addElement("atm_name", "OR", "ai.NAME",
					DbTextUtil.getOperationType(repFilter.getNameAndAddress()), repFilter.getNameAndAddress(), false);
			querConstr.addSimpleExpression("atm_name_addr", ")", " ");
		}
		querConstr.setQueryTail(
				") ds GROUP BY ds.ATM_ID, ds.STAT_DATE, ds.curr_remaining, ds.curr_code, ds.denom_value ) ds "
						+ "join T_CM_CURR ci on (ds.curr_code = ci.code_n3) "
						+ "GROUP BY ds.stat_date,ds.curr_code,ds.denom_value,ci.code_a3 "
						+ "order by curr_code,denom_value,stat_date");
		String query = querConstr.toString();
		if (repFilter.getAtmId() != null) {
			query.replaceFirst("//?", "#{atmId}");
		}
		if (repFilter.getNameAndAddress() != null) {
			query.replaceAll("//?", "#{nameAndAddr}");
		}
		return query;
	}
}
