package ru.bpc.cm.integration.orm.builders;

import java.util.List;
import java.util.Map;

import ru.bpc.cm.utils.db.JdbcUtils;

public class DataLoadBuilder {

	public String truncateTimestampBuilder(Map<String, Object> params) {
		String tableName = (String) params.get("tableName");
		String dateField = (String) params.get("dateField");
		StringBuilder sb = new StringBuilder("DELETE FROM ");
		sb.append(tableName).append(" WHERE ").append(tableName).append(".").append(dateField);
		sb.append(" >= #{dateFrom} and ").append(tableName).append(".").append(dateField).append(" <= #{dateTo}");
		return sb.toString();
	}

	public String deleteQueryBuilder(Map<String, Object> params) {
		String tableName = (String) params.get("tableName");
		StringBuilder sb = new StringBuilder("DELETE FROM ");
		sb.append(tableName).append(" WHERE atm_id = #{atmId} AND encashment_id < #{encId}");
		return sb.toString();
	}

	public String saveParamsForTaskBuilder_lastTransInfo(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Integer> atmList = (List<Integer>) params.get("atmList");
		StringBuffer lastTransInfoSQL = new StringBuffer(
				"SELECT COALESCE(MAX(utrnno),0) as lastUtrnno ,COALESCE(MAX(datetime),CURRENT_TIMESTAMP) as lastTransDatetime"
						+ " FROM t_cm_intgr_trans where ");
		lastTransInfoSQL.append(JdbcUtils.generateInConditionNumber("atm_id", atmList));
		return lastTransInfoSQL.toString();
	}
}
