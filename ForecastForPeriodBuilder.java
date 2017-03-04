package ru.bpc.cm.cashmanagement.orm.builders;

import java.util.List;
import java.util.Map;

import ru.bpc.cm.utils.CmUtils;

public class ForecastForPeriodBuilder {

	public String getStatDetailsCashInBuilder(Map<String, Object> params) {
		@SuppressWarnings("unchecked")
		List<Integer> encIds = (List<Integer>) params.get("encIds");
		StringBuffer sql = new StringBuffer(
				"select cs.ATM_ID,cs.STAT_DATE, cs.BILLS_COUNT as TAKE_OFF, cs.BILLS_REMAINING as CURR_REMAINING, "
						+ "dense_rank() over(partition by cs.STAT_DATE order by cs.CASH_IN_ENCASHMENT_ID) as RNK, "
						+ "count(1) over(partition by cs.STAT_DATE) as CNT from t_cm_cashin_stat cs "
						+ "where cs.atm_id = #{atmId} ");
		sql.append(CmUtils.getIdListInClause(encIds, "cs.cash_in_encashment_id"));
		sql.append(" AND cs.stat_Date > #{startDate} " + "AND cs.stat_Date <= #{endDate} "
				+ "order by cs.STAT_DATE,cs.CASH_IN_ENCASHMENT_ID");
		return sql.toString();
	}
}
