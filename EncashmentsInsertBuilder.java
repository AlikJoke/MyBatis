package ru.bpc.cm.cashmanagement.orm.builders;

import java.util.List;
import java.util.Map;

import ru.bpc.cm.utils.CmUtils;

public class EncashmentsInsertBuilder {

	public String deleteEncashmentsBuilder_encPlanDenom(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Integer> encList = (List<Integer>) params.get("encList");
		String table = (String) params.get("table");

		String deleteEncListClause = CmUtils.getIdListInClause(encList, "ENC_PLAN_ID");

		StringBuffer sb = new StringBuffer("DELETE FROM ");
		sb.append(table);
		sb.append(" WHERE 1=1 ");
		sb.append(deleteEncListClause);
		return sb.toString();
	}
	
	public String getExistingPlanIdBuilder(Map<String, Object> params) {
		String limit = (String) params.get("limit");
		return "SELECT * FROM ( SELECT ENC_PLAN_ID as FROM T_CM_ENC_PLAN " + "WHERE ATM_ID = #{atmId} "
				+ "AND DATE_FORTHCOMING_ENCASHMENT >= #{dateForCheck} "
				+ "AND IS_APPROVED = 0 ORDER BY ENC_PLAN_ID DESC) " + " " + limit + " ";
	}
}
