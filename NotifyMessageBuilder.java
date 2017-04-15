package ru.bpc.cm.cashmanagement.orm.builders;

import java.sql.Date;
import java.util.Map;

public class NotifyMessageBuilder {

	public String insertNotifyMessageBuilder(Map<String, Object> params) {
		String nextSeq = (String) params.get("nextSeq");
		return "INSERT INTO t_cm_notification_message (ID,CREATE_DATE,USER_ID,MESSAGE_TYPE,PARAMS,IS_NEW) " + "VALUES ("
				+ nextSeq + ",#{currTime},#{userId},#{msgId},#{printedCollection},1)";
	}

	public String getNotifyMessageListBuilder(Map<String, Object> params) {
		Boolean newOnly = (Boolean) params.get("newOnly");
		Date dateFrom = (Date) params.get("dateFrom");
		StringBuilder sql = new StringBuilder("SELECT ID,CREATE_DATE,MESSAGE_TYPE,PARAMS,IS_NEW "
				+ "FROM t_cm_notification_message WHERE USER_ID = #{userId}");
		if (newOnly) {
			sql.append(" AND IS_NEW = 1 ");
		}
		if (dateFrom != null)
			sql.append(" AND CREATE_DATE >= #{dateFrom}");

		sql.append("ORDER BY CREATE_DATE DESC");
		return sql.toString();
	}
}
