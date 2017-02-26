package ru.bpc.cm.cashmanagement.orm.builders;

import java.util.List;
import java.util.Map;

import ru.bpc.cm.utils.CmUtils;

public class CmCommonBuilder {

	public String getConvertionsRatesBuilder(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Integer> currencies = (List<Integer>) params.get("currencies");
		StringBuilder sql = new StringBuilder("select " + "rates.src_curr_code, " + "rates.dest_curr_code, "
				+ "rates.cnvt_rate, " + "rates.multiple_flag " + "from v_cm_curr_convert_rate rates "
				+ "join t_cm_atm a on " + " (rates.dest_inst_id = a.inst_id and rates.src_inst_id = a.inst_id) "
				+ "where a.ATM_ID = #{atmId} ");
		sql.append(CmUtils.getIdListInClause(currencies, "rates.src_curr_code"));
		sql.append(CmUtils.getIdListInClause(currencies, "rates.dest_curr_code"));
		return sql.toString();
	}
}
