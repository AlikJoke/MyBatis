package ru.bpc.cm.integration.orm.builders;

import java.util.Map;

public class DbJobBuilder {

	public String updateQueryBuilder(Map<String, Object> params) {
		String tableName = (String) params.get("tableName");
		String paramName = (String) params.get("paramName");

		StringBuilder sb = new StringBuilder("UPDATE ");
		sb.append(tableName).append(" SET ").append(paramName).append(" = ").append(paramName).append(" + #{shift}");
		return sb.toString();
	}

	public String deleteQueryBuilder(Map<String, Object> params) {
		String tableName = (String) params.get("tableName");
		String paramName = (String) params.get("paramName");

		StringBuilder sb = new StringBuilder("DELETE FROM ");
		sb.append(tableName).append(" WHERE ").append(paramName).append(" >= sysdate");
		return sb.toString();
	}
}
