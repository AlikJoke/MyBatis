package ru.bpc.cm.integration.orm.builders;

import java.util.List;
import java.util.Map;

import ru.bpc.cm.utils.db.JdbcUtils;

public class AggregationBuilder {

	public String deleteCashOutQueryBuilder(Map<String, Object> params) {
		String tableName = (String) params.get("tableName");
		StringBuilder sb = new StringBuilder();
		sb.append("DELETE FROM ");
		sb.append(tableName);
		sb.append(" WHERE  ATM_ID = #{atmId} AND ENCASHMENT_ID = #{encId} AND STAT_DATE >= #{statDate}");
		return sb.toString();
	}

	public String deleteCashInQueryBuilder(Map<String, Object> params) {
		String tableName = (String) params.get("tableName");
		StringBuilder sb = new StringBuilder();
		sb.append("DELETE FROM ");
		sb.append(tableName);
		sb.append(" WHERE  ATM_ID = #{atmId} AND CASH_IN_ENCASHMENT_ID = #{encId} AND STAT_DATE >= #{statDate}");
		return sb.toString();
	}

	public String prepareDowntimesComplex(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Integer> atmList = (List<Integer>) params.get("atmList");
		StringBuilder sb = new StringBuilder("select PID,START_DATE,COALESCE(END_DATE, CURRENT_TIMESTAMP) as END_DATE "
				+ "FROM t_cm_intgr_downtime_period "
				+ "WHERE DOWNTIME_TYPE_IND in (#{offlineType}, #{cashType}) and (");
		sb.append(JdbcUtils.generateInConditionNumber("pid", atmList));
		sb.append(") ");
		return sb.toString();
	}
	
	public String simpleQueryBuilder(Map<String, Object> params) {
		String query = (String) params.get("query");
		return query;
	}
}
