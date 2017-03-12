package ru.bpc.cm.cashmanagement.orm.builders;

import java.util.List;
import java.util.Map;

import ru.bpc.cm.items.encashments.AtmEncSubmitFilter;
import ru.bpc.cm.utils.CmUtils;

public class AtmEncashmentBuilder {

	public String getAtmEncashmentListBuilder(Map<String, Object> params) {
		AtmEncSubmitFilter filter = (AtmEncSubmitFilter) params.get("addFilter");
		StringBuilder sql = new StringBuilder("SELECT " + "ENC_PLAN_ID, aep.ATM_ID, aep.DATE_PREVIOUS_ENCASHMENT,"
				+ " aep.DATE_FORTHCOMING_ENCASHMENT, " + " IS_APPROVED, EMERGENCY_ENCASHMENT,CONFIRM_ID, "
				+ " u.NAME as APPROVE_NAME,u2.NAME as CONFIRM_NAME, "
				+ " u.LOGIN as APPROVE_LOGIN, u2.LOGIN as CONFIRM_LOGIN, "
				+ " ai.STATE,ai.CITY,ai.STREET,ai.NAME as ATM_NAME,aepr.NAME as REQUEST_NAME,aepr.ID as REQUEST_ID, "
				+ " aep.CASH_ADD_ENCASHMENT,aep.ENCASHMENT_TYPE, ai.EXTERNAL_ATM_ID " + "FROM T_CM_ENC_PLAN aep "
				+ "join T_CM_ATM ai on (ai.ATM_ID = aep.ATM_ID) " + "join T_CM_USER u on (u.ID = aep.APPROVE_ID)  "
				+ "left outer join T_CM_USER u2 on (u2.ID = aep.CONFIRM_ID)  "
				+ "left outer join T_CM_ENC_PLAN_REQUEST aepr on (aepr.ID = aep.ENC_REQ_ID)  " + "WHERE "
				+ "DATE_FORTHCOMING_ENCASHMENT >= #{dateFrom} " + "AND " + "DATE_FORTHCOMING_ENCASHMENT <= #{dateTo} ");
		if (filter.getPersonID() != 0) {
			sql.append("AND APPROVE_ID = #{personId} ");
		} else {
			sql.append(CmUtils.getIdentifiableListInClause(filter.getPersonList(), "APPROVE_ID"));
		}
		if (!filter.isShowConfirmed()) {
			sql.append("AND CONFIRM_ID = 0 ");
		}
		sql.append(" AND IS_APPROVED = 1");
		sql.append(" ORDER BY DATE_FORTHCOMING_ENCASHMENT");
		return sql.toString();
	}

	@SuppressWarnings("unchecked")
	public String getCurrDenomBuilder(Map<String, Object> params) {
		List<Integer> encPlanIDList = (List<Integer>) params.get("encPlanIDList");
		StringBuffer sql = new StringBuffer("SELECT DISTINCT DENOM_CURR " + "FROM T_CM_ENC_PLAN_DENOM " + "WHERE 1=1 ");
		sql.append(CmUtils.getIdListInClause(encPlanIDList, "ENC_PLAN_ID"));
		return sql.toString();
	}

	public String approveSelectedEncahmentsBuilder(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Integer> selectedEncashments = (List<Integer>) params.get("selected");
		StringBuffer sql = new StringBuffer("UPDATE " + "T_CM_ENC_PLAN " + "SET IS_APPROVED = 1, "
				+ "APPROVE_ID = #{personId} " + "WHERE " + "1 = 1 ");
		sql.append(CmUtils.getIdListInClause(selectedEncashments, "ENC_PLAN_ID"));
		return sql.toString();
	}

	public String confirmSelectedEncahmentsBuilder(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Integer> selectedEncashments = (List<Integer>) params.get("selected");
		StringBuffer sql = new StringBuffer(
				"UPDATE " + "T_CM_ENC_PLAN " + "SET " + "CONFIRM_ID = #{personId} " + "WHERE " + "1 = 1 ");
		sql.append(CmUtils.getIdListInClause(selectedEncashments, "ENC_PLAN_ID"));
		return sql.toString();
	}

	public String discardEncashmentsDateChangeBuilder(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Integer> selectedEncashments = (List<Integer>) params.get("selected");
		StringBuffer sql = new StringBuffer(
				"UPDATE " + "T_CM_ENC_PLAN " + "SET " + "IS_APPROVED = 0," + "CONFIRM_ID = 0 " + "WHERE " + "1 = 1 ");
		sql.append(CmUtils.getIdListInClause(selectedEncashments, "ENC_PLAN_ID"));
		return sql.toString();
	}

	public String updateReqIdForSelectedEncashmnetsBuilder(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Integer> selectedEncashments = (List<Integer>) params.get("selected");
		StringBuffer sql = new StringBuffer(
				"UPDATE " + "T_CM_ENC_PLAN " + "SET ENC_REQ_ID = #{encReqId} " + "WHERE " + "1 = 1 ");
		sql.append(CmUtils.getIdListInClause(selectedEncashments, "ENC_PLAN_ID"));
		return sql.toString();
	}

	public String updateReqDateForSelectedEncashmnetsEncashmnetsBuilder(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Integer> selectedEncashments = (List<Integer>) params.get("selected");
		StringBuffer sql = new StringBuffer("UPDATE " + "T_CM_ENC_PLAN "
				+ "SET DATE_FORTHCOMING_ENCASHMENT = #{reqDate} + (DATE_FORTHCOMING_ENCASHMENT - trunc(DATE_FORTHCOMING_ENCASHMENT))  "
				+ "WHERE " + "1 = 1 ");
		sql.append(CmUtils.getIdListInClause(selectedEncashments, "ENC_PLAN_ID"));
		return sql.toString();
	}
}
