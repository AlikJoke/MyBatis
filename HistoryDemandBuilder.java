package ru.bpc.cm.cashmanagement.orm.builders;

import java.util.Map;

public class HistoryDemandBuilder {

	public String getDemandCompareForAtmBuilder(Map<String, Object> params) {
		String statViewName = (String) params.get("statViewName");
		return "with st as (select st.atm_id, trunc(st.stat_date) as stat_date, sum(CURR_SUMM) as STAT_SUMM,"
				+ "st.curr_code from " + statViewName + " st where st.ATM_ID = #{atmId} "
				+ "and st.stat_Date >= #{dateFrom} and st.stat_date < #{dateTo} and st.CURR_CODE = #{curr} "
				+ "group by st.atm_id,trunc(st.stat_date),st.curr_code) select st.atm_id,st.stat_date,"
				+ "st.STAT_SUMM, adh.DEMAND as FORECASTED_SUMM, cl.USER_DISABLED as DISABLED_CNT from st "
				+ "join (select atm_id, trunc(cl_date) as cl_date, currency, currency_mode, sum(demand) as demand  from t_cm_atm_demand_history group by atm_id, trunc(cl_date), currency, currency_mode) adh on "
				+ "(st.atm_id = adh.atm_id and adh.cl_date = st.stat_date and adh.currency = st.curr_code) "
				+ "left outer join (select atm_id, trunc(cl_date) as cl_date, currency, currency_mode, MIN(user_disabled) as user_disabled from t_cm_atm_calendar_days group by atm_id, trunc(cl_date), currency, currency_mode) cl on "
				+ "(cl.atm_id =st.atm_id and cl.cl_date = st.stat_date and cl.CURRENCY_MODE = adh.CURRENCY_MODE "
				+ "and cl.currency = st.curr_code) where adh.CURRENCY_MODE = #{modeId} " + "order by stat_date";
	}

	public String getDemandHourCompareForAtmBuilder(Map<String, Object> params) {
		String statViewName = (String) params.get("statViewName");
		return "with st as (select vst.atm_id, vst.stat_date as stat_date, vst.CURR_SUMM as STAT_SUMM, "
				+ "vst.CURR_CODE as CURR_CODE from " + statViewName + " vst where vst.ATM_ID = #{atmId} "
				+ "and trunc(vst.stat_Date) = #{day} and vst.CURR_CODE = #{curr} ) select st.atm_id,st.stat_date,"
				+ "st.STAT_SUMM, adh.DEMAND as FORECASTED_SUMM, cl.USER_DISABLED as DISABLED_CNT from st "
				+ "join t_cm_atm_demand_history adh on (st.atm_id = adh.atm_id and adh.cl_date = st.stat_date "
				+ "and adh.currency = st.curr_code) left outer join t_cm_atm_calendar_days cl on "
				+ "(cl.atm_id =st.atm_id and cl.cl_date = st.stat_date and cl.CURRENCY_MODE = adh.CURRENCY_MODE "
				+ "and cl.currency = st.curr_code) where adh.CURRENCY_MODE = #{modeId} " + "order by stat_date";
	}
}
