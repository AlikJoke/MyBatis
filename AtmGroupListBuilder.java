package ru.bpc.cm.cashmanagement.orm.builders;

import java.util.Map;

import ru.bpc.cm.items.enums.AtmGroupType;
import ru.bpc.cm.items.settings.GroupFilter;
import ru.bpc.cm.utils.db.DbTextUtil;
import ru.bpc.cm.utils.db.QueryConstructor;

public class AtmGroupListBuilder {

	public String getAtmGroupListBuilder(Map<String, Object> params) {
		String personId = (String) params.get("personId");
		Boolean fetchSubordinatesGroups = (Boolean) params.get("fetchSubordinatesGroups");
		StringBuilder sql = new StringBuilder(
				"SELECT t.ID,t.NAME, t.DESCRIPTION,t.TYPE_ID FROM T_CM_ATM_GROUP t " + "WHERE t.TYPE_ID = #{typeId} ");
		if (personId != null) {
			sql.append("AND ( ");
			sql.append(getUserGroupClause("t.ID"));
			if (fetchSubordinatesGroups) {
				sql.append(getSubordinatsGroupClause("t.ID"));
			}
			sql.append(") ");
		}
		sql.append(" order by name ");
		return sql.toString();
	}

	public static QueryConstructor getAtmGroupListBuilder(GroupFilter filter) {
		String sql = "SELECT t.ID,t.NAME, t.DESCRIPTION,t.TYPE_ID " + "FROM T_CM_ATM_GROUP t " + "WHERE t.ID in ( "
				+ "SELECT tg.ID FROM T_CM_ATM_GROUP tg  "
				+ "left outer join T_CM_ATM2ATM_GROUP tr on ( tg.ID = tr.ATM_GROUP_ID ) "
				+ "left outer join T_CM_ATM ta on (tr.ATM_ID = ta.ATM_ID) " + "where 1=1 "
				+ "AND tg.ID in (select ID from T_CM_TEMP_ATM_GROUP_LIST) ";
		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql, true);
		querConstr.addElementIfNotNull("atm_id", "AND", "tr.atm_id", DbTextUtil.getOperationType(filter.getAtmID()),
				filter.getAtmID());
		querConstr.addElementIfNotNull("atm_name", "AND", "ta.name", DbTextUtil.getOperationType(filter.getAtmName()),
				filter.getAtmName());
		querConstr.addElementIfNotNull("group_name", "AND", "tg.name",
				DbTextUtil.getOperationType(filter.getGroupName()), filter.getGroupName());
		querConstr.addElementIfNotNull("group_id", "AND", "tg.ID", DbTextUtil.getOperationType(filter.getGroupID()),
				filter.getGroupID());
		querConstr.addElementIfNotNull("type2", "AND", "tg.TYPE_ID", "=", AtmGroupType.USER_TO_ATM.getId());
		querConstr.setQueryTail(" ) ORDER BY t.NAME");
		return querConstr;
	}

	public String getAtmGroupIdListBuilder(Map<String, Object> params) {
		String personId = (String) params.get("personId");
		Boolean fetchSubordinatesGroups = (Boolean) params.get("fetchSubordinatesGroups");
		StringBuilder sql = new StringBuilder("SELECT t.ID FROM T_CM_ATM_GROUP t " + "WHERE t.TYPE_ID = #{typeId} ");
		if (personId != null) {
			sql.append("AND ( ");
			sql.append(getUserGroupClause("t.ID"));
			if (fetchSubordinatesGroups) {
				sql.append(getSubordinatsGroupClause("t.ID"));
			}
			sql.append(") ");
		}
		return sql.toString();
	}

	public String getAtmGroupListForAtmBuilder(Map<String, Object> params) {
		String personId = (String) params.get("personId");
		Boolean fetchSubordinatesGroups = (Boolean) params.get("fetchSubordinatesGroups");
		StringBuilder sql = new StringBuilder("SELECT " + " ag.name,ag.description " + "FROM "
				+ "T_CM_ATM2ATM_GROUP agr " + "join T_CM_ATM_GROUP ag on(ag.ID = agr.ATM_GROUP_ID) " + "WHERE "
				+ "ag.type_id=#{typeId} AND agr.ATM_ID =#{atmId} ");
		if (personId != null) {
			sql.append("AND ( ");
			sql.append(getUserGroupClause("agr.atm_group_id"));
			if (fetchSubordinatesGroups) {
				sql.append(getSubordinatsGroupClause("ag.ID"));
			}

			sql.append(" ) ");
		}
		sql.append(" ORDER BY name");
		return sql.toString();
	}

	private static String getUserGroupClause(String keyword) {
		StringBuilder sql = new StringBuilder(keyword);
		sql.append(" IN (SELECT ATM_GROUP_ID FROM T_cm_USER2ATM_GROUPS WHERE USER_ID = #{personId} )");
		return sql.toString();
	}

	private static String getSubordinatsGroupClause(String keyword) {
		StringBuilder sql = new StringBuilder("OR ");
		sql.append(keyword);
		sql.append(" IN (SELECT DISTINCT wa.ATM_GROUP_ID " + "FROM T_cm_USER2USERS wu "
				+ "join T_cm_USER2ATM_GROUPS wa on (wa.USER_ID = wu.SUB_USER_ID) "
				+ "WHERE wu.USER_ID = #{personId} )");
		return sql.toString();
	}
}
