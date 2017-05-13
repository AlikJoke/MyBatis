package ru.bpc.cm.tasks.orm.builders;

import java.sql.SQLException;
import java.util.Map;

import org.apache.ibatis.session.SqlSession;

import ru.bpc.cm.orm.common.ORMUtils;

public class WebTaskBuilder {

	public String getTaskIdForChangeWebTaskBuilder(Map<String, Object> params) throws SQLException {
		SqlSession session = (SqlSession) params.get("session");
		return "select " + ORMUtils.getNextSequence(session, "s_web_task_id") + " as TASK_ID "
				+ ORMUtils.getFromDummyExpression(session);
	}

	public String getGroupListParamBuilder(Map<String, Object> params) throws SQLException {
		StringBuilder sql = new StringBuilder(
				"SELECT " + "id,description " + "FROM " + "T_CM_ATM_GROUP " + "WHERE " + "ID IN (");
		sql.append("#{typeId}");
		sql.append(")  ORDER BY description");
		return sql.toString();
	}
}
