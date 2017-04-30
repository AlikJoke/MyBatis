package ru.bpc.cm.integration.orm.builders;

import java.util.Map;

public class DataLoadBuilder {

	public String truncateTimestampBuilder(Map<String, Object> params) {
		String tableName = (String) params.get("tableName");
		String dateField = (String) params.get("dateField");
		StringBuilder sb = new StringBuilder("DELETE FROM ");
		sb.append(tableName).append(" WHERE ").append(tableName).append(".").append(dateField);
		sb.append(" >= #{dateFrom} and ").append(tableName).append(".").append(dateField).append(" <= #{dateTo}");
		return sb.toString();
	}
}
