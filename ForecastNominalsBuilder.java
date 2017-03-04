package ru.bpc.cm.cashmanagement.orm.builders;

import java.util.List;
import java.util.Map;

import ru.bpc.cm.utils.CmUtils;

public class ForecastNominalsBuilder {

	public String getCurrNominalsBuilder(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Integer> encList = (List<Integer>) params.get("encList");
		String deleteEncListClause = CmUtils.getIdListInClause(encList, "ENCASHMENT_ID");

		StringBuffer sql = new StringBuffer("SELECT " + "COUNT(DISTINCT STAT_DATE) as COUNT_DAYS, "
				+ "SUM(DENOM_TRANS_COUNT) as COUNT_TRANS, " + "SUM(DENOM_COUNT) as DENOM_COUNT, "
				+ "DENOM_VALUE, DENOM_CURR " + "FROM V_CM_CASHOUT_DENOM_STAT ads " + "WHERE DENOM_CURR = #{curr} "
				+ "AND ATM_ID = #{atmId} ");
		sql.append(deleteEncListClause);
		sql.append("AND NOT EXISTS ( " + "SELECT NULL FROM " + "V_CM_CASHOUT_DENOM_STAT "
				+ "WHERE DENOM_CURR = #{curr} " + "AND STAT_DATE = ads.STAT_DATE " + "AND ATM_ID = #{atmId} ");
		sql.append(deleteEncListClause);
		sql.append("AND DENOM_REMAINING = 0)" + " GROUP BY DENOM_VALUE ");
		return sql.toString();
	}

	public String getCoCurrNominalsFromAtmCassettesBuilder_count(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Integer> encList = (List<Integer>) params.get("encList");
		String deleteEncListClause = CmUtils.getIdListInClause(encList, "ENCASHMENT_ID");

		StringBuffer sql = new StringBuffer(
				"SELECT " + "COUNT(DISTINCT STAT_DATE) as COUNT_DAYS, " + "SUM(DENOM_TRANS_COUNT) as COUNT_TRANS, "
						+ "SUM(DENOM_COUNT) as DENOM_COUNT " + "FROM V_CM_CASHOUT_DENOM_STAT ads "
						+ "WHERE DENOM_CURR = #{curr} " + "AND DENOM_VALUE = #{denom} " + "AND ATM_ID = #{atmId} ");
		sql.append(deleteEncListClause);
		sql.append("AND NOT EXISTS ( " + "SELECT NULL FROM " + "V_CM_CASHOUT_DENOM_STAT "
				+ "WHERE DENOM_CURR = #{curr} " + "AND STAT_DATE = ads.STAT_DATE " + "AND ATM_ID = #{atmId} ");
		sql.append(deleteEncListClause);
		sql.append("AND DENOM_REMAINING = 0)");
		return sql.toString();
	}

	public String getCrCurrNominalsFromAtmCassettesBuilder_count(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Integer> encList = (List<Integer>) params.get("encList");
		String deleteEncListClause = CmUtils.getIdListInClause(encList, "CASH_IN_ENCASHMENT_ID");

		StringBuffer sql = new StringBuffer("SELECT " + "COUNT(DISTINCT STAT_DATE) as COUNT_DAYS, "
				+ "SUM(DENOM_TRANS_COUNT_OUT) as COUNT_TRANS, "
				+ "SUM(DENOM_COUNT_OUT) - SUM(DENOM_COUNT_IN) as DENOM_COUNT " + "FROM V_CM_CASHIN_R_DENOM_STAT ads "
				+ "WHERE DENOM_CURR = #{curr} " + "AND DENOM_VALUE = #{denom} " + "AND ATM_ID = #{atmId} ");
		sql.append(deleteEncListClause);
		sql.append("AND NOT EXISTS ( " + "SELECT NULL FROM " + "V_CM_CASHIN_R_DENOM_STAT "
				+ "WHERE DENOM_CURR = #{curr} " + "AND STAT_DATE = ads.STAT_DATE " + "AND ATM_ID = #{atmId} ");
		sql.append(deleteEncListClause);
		sql.append("AND DENOM_REMAINING = 0)");
		return sql.toString();
	}

}
