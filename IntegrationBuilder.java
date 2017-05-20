package ru.bpc.cm.integration.orm.builders;

import java.util.List;
import java.util.Map;

import ru.bpc.cm.utils.db.JdbcUtils;

public class IntegrationBuilder {

	public String simpleQueryBuilder(Map<String, Object> params) {
		String query = (String) params.get("query");
		return query;
	}

	public String loadAtmTrans_getIntgrLastUtrnnoComplexBuilder(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Integer> vAtmList = (List<Integer>) params.get("vAtmList");
		StringBuilder intgrLastUtrnno = new StringBuilder(
				"SELECT MIN(utrnno)-1 as res1,MAX(utrnno) as res2 " + "FROM t_cm_intgr_trans where ");
		intgrLastUtrnno.append(JdbcUtils.generateInConditionNumber("atm_id", vAtmList));

		return intgrLastUtrnno.toString();
	}
}
