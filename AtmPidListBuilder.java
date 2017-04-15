package ru.bpc.cm.cashmanagement.orm.builders;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import ru.bpc.cm.management.Institute;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.TextUtil;
import ru.bpc.cm.utils.db.DbTextUtil;
import ru.bpc.cm.utils.db.QueryConstructor;

public class AtmPidListBuilder {

	public String getAtmListSplitByGroupListDescxBuilder1(Map<String, Object> params) throws SQLException {
		String atmIdFilter = (String) params.get("atmIdFilter");
		String nameAndAddressFilter = (String) params.get("nameAndAddressFilter");
		StringBuffer sql = new StringBuffer("SELECT " + "agr.atm_id, " + "agr.atm_group_id, "
				+ "ag.name as ATM_GROUP_NAME, " + "aig.name as ATM_NAME, " + "aig.external_atm_id " + "FROM "
				+ "t_cm_atm2atm_group agr " + "JOIN t_cm_atm_group ag ON (ag.id = agr.atm_group_id) "
				+ "JOIN t_cm_atm aig ON (aig.atm_id = agr.atm_id) "
				+ "WHERE agr.atm_id in (select id from t_cm_temp_atm_list) "
				+ "AND agr.atm_group_id in (select id from t_cm_temp_atm_group_list) ");
		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql.toString(), true);
		querConstr.addElementIfNotNull("atm_id", "AND", "agr.atm_id", DbTextUtil.getOperationType(atmIdFilter),
				atmIdFilter);
		if (TextUtil.isNotEmpty(nameAndAddressFilter)) {
			querConstr.addElement("address", "AND ( ", "aig.state || ', ' || aig.city || ', ' || aig.street",
					DbTextUtil.getOperationType(nameAndAddressFilter), nameAndAddressFilter, false);
			querConstr.addElement("atm_name", "OR", "aig.NAME", DbTextUtil.getOperationType(nameAndAddressFilter),
					nameAndAddressFilter, false);
			querConstr.addSimpleExpression("atm_name_addr", ")", " ");
		}
		querConstr.setQueryTail(" ORDER BY ATM_GROUP_NAME, atm_id");
		String query = querConstr.getQuery();
		return query.replaceFirst("?", "#{atmIdFilter").replaceAll("?", "#{nameAndAddressFilter}");
	}

	public String getAtmListSplitByGroupListDescxBuilder2(Map<String, Object> params) throws SQLException {
		String atmId = (String) params.get("atmId");
		String nameAndAddress = (String) params.get("nameAndAddress");
		StringBuffer sql = new StringBuffer("SELECT " + "agr.atm_id, " + "agr.atm_group_id, "
				+ "ag.name as ATM_GROUP_NAME, " + "aig.name as ATM_NAME, " + "aig.external_atm_id " + "FROM "
				+ "t_cm_atm2atm_group agr " + "JOIN t_cm_atm_group ag ON (ag.id = agr.atm_group_id) "
				+ "JOIN t_cm_atm aig ON (aig.atm_id = agr.atm_id) "
				+ "WHERE agr.atm_id in (select id from t_cm_temp_atm_list) "
				+ "AND agr.atm_group_id in (select id from t_cm_temp_atm_group_list) ");
		QueryConstructor querConstr = new QueryConstructor();
		querConstr.setQueryBody(sql.toString(), true);
		querConstr.addElementIfNotNull("atm_id", "AND", "agr.atm_id", DbTextUtil.getOperationType(atmId), atmId);
		if (TextUtil.isNotEmpty(nameAndAddress)) {
			querConstr.addElement("address", "AND ( ", "aig.state || ', ' || aig.city || ', ' || aig.street",
					DbTextUtil.getOperationType(nameAndAddress), nameAndAddress, false);
			querConstr.addElement("atm_name", "OR", "aig.NAME", DbTextUtil.getOperationType(nameAndAddress),
					nameAndAddress, false);
			querConstr.addSimpleExpression("atm_name_addr", ")", " ");
		}
		querConstr.setQueryTail(" ORDER BY ATM_GROUP_NAME, atm_id");
		String query = querConstr.getQuery();
		return query.replaceFirst("\\?", "#{atmId}").replaceAll("\\?", "#{nameAndAddress}");
	}

	public String getAtmListSplitByGroupBuilder(Map<String, Object> params) {
		Boolean filterGroupsList = (Boolean) params.get("filterGroupsList");
		@SuppressWarnings("unchecked")
		List<Institute> instList = (List<Institute>) params.get("instList");
		StringBuilder sql = new StringBuilder(
				"SELECT agr.ATM_ID, a.NAME as ATM_NAME,agr.ATM_GROUP_ID,ag.NAME,a.external_atm_id " + "FROM "
						+ "T_CM_ATM2ATM_GROUP agr " + "join T_CM_ATM_GROUP ag on(ag.ID = agr.ATM_GROUP_ID) "
						+ "join T_CM_ATM a on (agr.ATM_ID=a.ATM_ID) " + "WHERE " + "ag.TYPE_ID = #{id} ");
		if (filterGroupsList) {
			sql.append("AND agr.atm_group_id in (select id from t_cm_temp_atm_group_list)");
		}
		sql.append(CmUtils.getInstListInClause(instList, "a.INST_ID"));
		sql.append(" ORDER BY name,ATM_ID");
		return sql.toString();
	}

	public String getAtmListForGroupListBuilder(Map<String, Object> params) {
		Boolean sortByOutOfCurrDate = (Boolean) params.get("sortByOutOfCurrDate");
		StringBuilder sql = new StringBuilder(
				"SELECT " + "distinct a2ag.atm_id, a.NAME as ATM_NAME, a.EXTERNAL_ATM_ID, "
						+ "LEAST(aas.OUT_OF_CASH_IN_DATE,aas.OUT_OF_CASH_OUT_DATE) as SORT_DATE " + "FROM "
						+ "t_cm_atm2atm_group a2ag " + "join t_cm_atm a on (a2ag.atm_id = a.atm_id) "
						+ "left outer join t_cm_atm_actual_state aas on (a2ag.atm_id = aas.atm_id) " + "WHERE "
						+ "a2ag.atm_group_id in (select id from t_cm_temp_atm_group_list) ");
		sql.append(sortByOutOfCurrDate ? " ORDER BY sort_date" : " ORDER BY atm_id");
		return sql.toString();
	}

	public String getAtmListForGroupListBuilder_inst(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Institute> instList = (List<Institute>) params.get("instList");
		Boolean sortByOutOfCurrDate = (Boolean) params.get("sortByOutOfCurrDate");
		StringBuilder sql = new StringBuilder(
				"SELECT " + "distinct a2ag.atm_id, a.NAME as ATM_NAME, a.EXTERNAL_ATM_ID, "
						+ "LEAST(aas.OUT_OF_CASH_IN_DATE,aas.OUT_OF_CASH_OUT_DATE) as SORT_DATE " + "FROM "
						+ "t_cm_atm2atm_group a2ag " + "join t_cm_atm a on (a2ag.atm_id = a.atm_id) "
						+ "left outer join t_cm_atm_actual_state aas on (a2ag.atm_id = aas.atm_id) " + "WHERE "
						+ "a2ag.atm_group_id in (select id from t_cm_temp_atm_group_list)");
		sql.append(CmUtils.getInstListInClause(instList, "a.inst_id"));
		sql.append(sortByOutOfCurrDate ? " ORDER BY sort_date" : " ORDER BY atm_id");
		return sql.toString();
	}

	public String getAtmListForGroupBuilder_inst(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Institute> instList = (List<Institute>) params.get("instList");
		StringBuilder sql = new StringBuilder(
				"SELECT agr.ATM_ID as ATM_ID,ai.EXTERNAL_ATM_ID,ai.INST_ID,ai.STATE,ai.CITY,ai.STREET,ai.NAME as ATM_NAME "
						+ "FROM T_CM_ATM2ATM_GROUP agr " + "join T_CM_ATM ai on (ai.ATM_ID = agr.ATM_ID) "
						+ "WHERE agr.ATM_GROUP_ID = #{groupId} ");
		sql.append(CmUtils.getInstListInClause(instList, "a.inst_id"));
		sql.append(" ORDER BY atm_id ");
		return sql.toString();
	}
	
	public String getAtmListForGroupBuilder(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Institute> instList = (List<Institute>) params.get("instList");
		StringBuilder sql = new StringBuilder(
				"SELECT agr.ATM_ID as ATM_ID,ai.EXTERNAL_ATM_ID,ai.INST_ID,ai.STATE,ai.CITY,ai.STREET,ai.NAME as ATM_NAME "
						+ "FROM T_CM_ATM2ATM_GROUP agr " + "join T_CM_ATM ai on (ai.ATM_ID = agr.ATM_ID) "
						+ "WHERE agr.ATM_GROUP_ID = #{groupId} ");
		sql.append(CmUtils.getInstListInClause(instList, "ai.inst_id"));
		sql.append(" ORDER BY atm_id ");
		return sql.toString();
	}

	public String getAtmListForUserBuilder(Map<String, Object> params) {
		String personId = (String) params.get("personId");
		StringBuilder sql = new StringBuilder(
				"SELECT ai.ATM_ID, ai.EXTERNAL_ATM_ID, ai.name as ATM_NAME " + "FROM T_CM_ATM ai ");
		if (personId != null) {
			sql.append("WHERE NOT EXISTS " + "(SELECT null " + "FROM T_CM_ATM2ATM_GROUP agr1 "
					+ "join T_CM_ATM_GROUP ag1 on (ag1.ID = agr1.ATM_GROUP_ID) "
					+ "join T_cm_USER2ATM_GROUPS uag on (uag.ATM_GROUP_ID = agr1.ATM_GROUP_ID) " + "WHERE "
					+ "uag.USER_ID != #{personId} " + "AND agr1.ATM_ID = ai.ATM_ID " + "AND ag1.TYPE_ID = #{typeId}) ");
		}
		sql.append(" ORDER BY atm_id ");
		return sql.toString();
	}

	public String getAtmListFullBuilder(Map<String, Object> params) {
		String personId = (String) params.get("personId");
		StringBuilder sql = new StringBuilder(
				"SELECT ai.ATM_ID,ai.EXTERNAL_ATM_ID,ai.INST_ID,ai.STATE,ai.CITY,ai.STREET,ai.NAME as ATM_NAME "
						+ "FROM T_CM_ATM ai ");
		if (personId != null) {
			sql.append("WHERE NOT EXISTS " + "(SELECT null " + "FROM T_CM_ATM2ATM_GROUP agr1 "
					+ "join T_CM_ATM_GROUP ag1 on (ag1.ID = agr1.ATM_GROUP_ID) "
					+ "join T_cm_USER2ATM_GROUPS uag on (uag.ATM_GROUP_ID = agr1.ATM_GROUP_ID) " + "WHERE "
					+ "uag.USER_ID != #{personId} " + "AND agr1.ATM_ID = ai.ATM_ID " + "AND ag1.TYPE_ID = #{typeId}) ");

			sql.append(
					"UNION SELECT agr.ATM_ID as ATM_ID,ai.EXTERNAL_ATM_ID,ai.INST_ID,ai.STATE,ai.CITY,ai.STREET,ai.NAME as ATM_NAME "
							+ "FROM T_CM_ATM2ATM_GROUP agr " + "join T_CM_ATM ai on (ai.ATM_ID = agr.ATM_ID) "
							+ "WHERE agr.ATM_GROUP_ID = #{groupId} ");
		}
		sql.append(" ORDER BY atm_id ");
		return sql.toString();
	}

	public String getAtmListFullBuilder_inst(Map<String, Object> params) {
		String personId = (String) params.get("personId");
		@SuppressWarnings("unchecked")
		List<Institute> instList = (List<Institute>) params.get("instList");
		StringBuilder sql = new StringBuilder(
				"SELECT ai.ATM_ID,ai.STATE,ai.CITY,ai.STREET,ai.NAME as ATM_NAME, ai.EXTERNAL_ATM_ID "
						+ "FROM T_CM_ATM ai " + "WHERE 1=1 ");
		if (personId != null) {
			sql.append("AND NOT EXISTS " + "(SELECT null " + "FROM T_CM_ATM2ATM_GROUP agr1 "
					+ "join T_CM_ATM_GROUP ag1 on (ag1.ID = agr1.ATM_GROUP_ID) "
					+ "join T_cm_USER2ATM_GROUPS uag on (uag.ATM_GROUP_ID = agr1.ATM_GROUP_ID) " + "WHERE "
					+ "uag.USER_ID != #{personId} " + "AND agr1.ATM_ID = ai.ATM_ID " + "AND ag1.TYPE_ID = #{typeId}) ");

			sql.append("UNION SELECT agr.ATM_ID as ATM_ID,ai.STATE,ai.CITY,ai.STREET " + "FROM T_CM_ATM2ATM_GROUP agr "
					+ "join T_CM_ATM ai on (ai.ATM_ID = agr.ATM_ID) " + "WHERE agr.ATM_GROUP_ID = #{groupId} ");
		}
		sql.append(CmUtils.getInstListInClause(instList, "ai.inst_id"));
		sql.append(" ORDER BY atm_id ");
		return sql.toString();
	}

	public String getAvaliableAtmsListForGroupBuilder(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Institute> instList = (List<Institute>) params.get("instList");
		StringBuilder sql = new StringBuilder(
				"SELECT ai.ATM_ID,ai.STATE,ai.CITY,ai.STREET,ai.NAME as ATM_NAME, ai.EXTERNAL_ATM_ID "
						+ "FROM T_CM_ATM ai ");

		sql.append("WHERE NOT EXISTS " + "(SELECT null " + "FROM T_CM_ATM2ATM_GROUP agr1 "
				+ "join T_CM_ATM_GROUP ag1 on (ag1.ID = agr1.ATM_GROUP_ID) " + "WHERE " + "ag1.ID = #{groupId} "
				+ "AND agr1.ATM_ID = ai.ATM_ID " + "AND ag1.TYPE_ID = #{typeId} )");

		sql.append(CmUtils.getInstListInClause(instList, "ai.inst_id"));
		sql.append(" ORDER BY atm_id ");
		return sql.toString();
	}

	public String getAvaliableAtmsListForAttributeGroupBuilder(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Institute> instList = (List<Institute>) params.get("instList");
		StringBuilder sql = new StringBuilder(
				"SELECT ai.ATM_ID,ai.STATE,ai.CITY,ai.STREET,ai.NAME as ATM_NAME, ai.EXTERNAL_ATM_ID "
						+ "FROM T_CM_ATM ai ");
		sql.append("WHERE NOT EXISTS " + "(SELECT null " + "FROM T_CM_ATM2ATM_GROUP agr1 "
				+ "join T_CM_ATM_GROUP ag1 on (ag1.ID = agr1.ATM_GROUP_ID) " + "WHERE " + "ag1.ID = #{groupId} "
				+ "AND agr1.ATM_ID = ai.ATM_ID " + "AND ag1.TYPE_ID != #{typeId} )");

		sql.append(CmUtils.getInstListInClause(instList, "ai.inst_id"));
		sql.append(" ORDER BY atm_id ");
		return sql.toString();
	}

	public String getCalendarAtmListBuilder_Adm(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Institute> instList = (List<Institute>) params.get("instList");
		StringBuilder sql = new StringBuilder(
				"select distinct a.atm_id,a.EXTERNAL_ATM_ID,DESCX,a.STATE,a.CITY,a.STREET,c.CL_ID,a.INST_ID, a.NAME as ATM_NAME  "
						+ "from t_cm_atm a " + "left outer join t_cm_calendar c on (a.CALENDAR_ID = c.CL_ID) "
						+ "WHERE 1=1 ");
		sql.append(CmUtils.getInstListInClause(instList, "a.INST_ID"));
		sql.append(" order by a.atm_id");
		return sql.toString();
	}

	public String getCalendarAtmListBuilder_User(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Institute> instList = (List<Institute>) params.get("instList");
		StringBuilder sql = new StringBuilder(
				"select distinct a.atm_id,a.EXTERNAL_ATM_ID,DESCX,a.STATE,a.CITY,a.STREET,c.CL_ID,a.INST_ID, a.NAME as ATM_NAME "
						+ "from t_cm_atm2atm_group a2ag " + "join t_cm_atm a on (a2ag.ATM_ID = a.ATM_ID) "
						+ "left outer join t_cm_calendar c on (a.CALENDAR_ID = c.CL_ID) "
						+ "WHERE a2ag.atm_group_id in (select id from t_cm_temp_atm_group_list) ");
		sql.append(CmUtils.getInstListInClause(instList, "a.INST_ID"));
		sql.append(" order by a.atm_id");
		return sql.toString();
	}
}
