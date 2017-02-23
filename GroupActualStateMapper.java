package ru.bpc.cm.monitoring.orm;

import java.util.List;

import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.utils.Pair;

/**
 * Маппер для {@link GroupStateController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 19.02.2007
 * @version 1.0.0
 *
 */
public interface GroupActualStateMapper extends IMapper {

	@ConstructorArgs({
		@Arg(column = "DENOM_COUNT", javaType = String.class),
		@Arg(column = "CODE_A3", javaType = String.class)
	})
	@Select(" SELECT sum(epd.DENOM_VALUE*epd.DENOM_COUNT) as DENOM_COUNT, epd.DENOM_CURR , ci.CODE_A3 "
			+ "FROM T_CM_ENC_PLAN ep join T_CM_ENC_PLAN_DENOM epd on(ep.ENC_PLAN_ID = epd.ENC_PLAN_ID) "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE "
			+ "trunc(ep.DATE_FORTHCOMING_ENCASHMENT) = #{date} AND ep.ATM_ID = #{atmId} AND ep.IS_APPROVED = 1 "
			+ "AND ep.APPROVE_ID > 0 AND ep.CONFIRM_ID > 0 GROUP BY epd.DENOM_CURR,ci.CODE_A3 " + "ORDER BY DENOM_CURR")
	@Options(useCache = true, fetchSize = 1000)
	List<Pair> getAtmEncPlanSums(@Param("date") java.sql.Date date, @Param("atmId") Integer atmId);

	@ConstructorArgs({
		@Arg(column = "DENOM_COUNT", javaType = String.class),
		@Arg(column = "CODE_A3", javaType = String.class)
	})
	@Select(" SELECT sum(epd.DENOM_VALUE*epd.DENOM_COUNT) as DENOM_COUNT, epd.DENOM_CURR , ci.CODE_A3 "
			+ "FROM T_CM_ENC_PERIOD ep join T_CM_ENC_PERIOD_DENOM epd on(ep.ID = epd.ENC_PERIOD_ID) "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE "
			+ "trunc(ep.DATE_FORTHCOMING_ENCASHMENT) >= #{dateFrom} AND trunc(ep.DATE_FORTHCOMING_ENCASHMENT) <= #{dateTo} "
			+ "AND ep.ATM_ID = #{atmId} GROUP BY epd.DENOM_CURR,ci.CODE_A3 ORDER BY DENOM_CURR")
	@Options(useCache = true, fetchSize = 1000)
	List<Pair> getAtmEncPeriodSums(@Param("dateFrom") java.sql.Date dateFrom, @Param("dateTo") java.sql.Date dateTo,
			@Param("atmId") Integer atmId);
	
	@ConstructorArgs({
		@Arg(column = "CURR_REMAINING", javaType = String.class),
		@Arg(column = "CODE_A3", javaType = String.class)
	})
	@Select(" SELECT SUM(curr_remaining) as CURR_REMAINING, CURR_CODE, CODE_A3 FROM ( "
			+ "SELECT ccus.curr_remaining,ccus.curr_code , ci.CODE_A3 FROM t_cm_atm_actual_state aas "
			+ "JOIN t_cm_cashout_curr_stat ccus ON ( aas.atm_id = ccus.atm_id "
			+ "AND ccus.encashment_id = aas.cash_out_encashment_id AND ccus.stat_date = aas.cash_out_stat_date) "
			+ "JOIN t_cm_curr ci on (ccus.curr_code = ci.code_n3) WHERE aas.atm_id = #{atmId} union "
			+ "SELECT ccus.curr_remaining,ccus.curr_code , ci.CODE_A3 FROM t_cm_atm_actual_state aas "
			+ "JOIN t_cm_cashin_r_curr_stat ccus ON ( aas.atm_id = ccus.atm_id "
			+ "AND ccus.cash_in_encashment_id = aas.cash_in_encashment_id "
			+ "AND ccus.stat_date = aas.cash_out_stat_date) JOIN t_cm_curr ci on (ccus.curr_code = ci.code_n3) "
			+ "WHERE aas.atm_id = #{atmId} ) GROUP BY CURR_CODE, CODE_A3 ORDER BY curr_code")
	@Options(useCache = true, fetchSize = 1000)
	List<Pair> getAtmRemainingSums(@Param("atmId") Integer atmId);
	
	@ConstructorArgs({
		@Arg(column = "DENOM_COUNT", javaType = String.class),
		@Arg(column = "CODE_A3", javaType = String.class)
	})
	@Select(" SELECT sum(epd.DENOM_VALUE*epd.DENOM_COUNT) as DENOM_COUNT, ci.CODE_A3 "
			+ "FROM T_CM_ENC_CASHOUT_STAT ep "
			+ "join V_CM_ENC_CASHOUT_STAT_DETAILS epd on(ep.ENCASHMENT_ID = epd.ENCASHMENT_ID and epd.ACTION_TYPE = 1) "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE trunc(ep.ENC_DATE) = #{date} "
			+ "AND ep.ATM_ID = #{atmId} AND epd.action_type = #{actionId} GROUP BY ci.CODE_A3 ORDER BY DENOM_COUNT DESC")
	@Options(useCache = true, fetchSize = 1000)
	List<Pair> getAtmEncStatSums(@Param("date") java.sql.Date date, @Param("atmId") Integer atmId,
			@Param("actionId") Integer actionId);
	
	@ConstructorArgs({
		@Arg(column = "DENOM_COUNT", javaType = String.class),
		@Arg(column = "CODE_A3", javaType = String.class)
	})
	@Select(" SELECT sum(epd.DENOM_VALUE*epd.DENOM_COUNT) as DENOM_COUNT, epd.DENOM_CURR , ci.CODE_A3 "
			+ "FROM T_CM_ENC_PLAN ep join T_CM_ENC_PLAN_DENOM epd on(ep.ENC_PLAN_ID = epd.ENC_PLAN_ID) "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE "
			+ "trunc(ep.DATE_FORTHCOMING_ENCASHMENT) = #{date} AND ep.atm_id in (select id from t_cm_temp_atm_list) "
			+ "AND ep.IS_APPROVED = 1 AND ep.APPROVE_ID > 0 AND ep.CONFIRM_ID > 0 "
			+ "GROUP BY epd.DENOM_CURR,ci.CODE_A3 ORDER BY DENOM_CURR")
	@Options(useCache = true, fetchSize = 1000)
	List<Pair> getAtmsEncPlanSums(@Param("date") java.sql.Date date);

	@ConstructorArgs({
		@Arg(column = "DENOM_COUNT", javaType = String.class),
		@Arg(column = "CODE_A3", javaType = String.class)
	})
	@Select(" SELECT sum(epd.DENOM_VALUE*epd.DENOM_COUNT) as DENOM_COUNT, epd.DENOM_CURR , ci.CODE_A3 "
			+ "FROM T_CM_ENC_PERIOD ep join T_CM_ENC_PERIOD_DENOM epd on(ep.ID = epd.ENC_PERIOD_ID) "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE "
			+ "trunc(ep.DATE_FORTHCOMING_ENCASHMENT) >= #{dateFrom} AND ep.atm_id in (select id from t_cm_temp_atm_list) "
			+ "AND trunc(ep.DATE_FORTHCOMING_ENCASHMENT) <= #{dateTo} GROUP BY epd.DENOM_CURR,ci.CODE_A3 "
			+ "ORDER BY DENOM_CURR")
	@Options(useCache = true, fetchSize = 1000)
	List<Pair> getAtmsEncPeriodSums(@Param("dateFrom") java.sql.Date dateFrom, @Param("dateTo") java.sql.Date dateTo);

	@ConstructorArgs({
		@Arg(column = "CURR_REMAINING", javaType = String.class),
		@Arg(column = "CODE_A3", javaType = String.class)
	})
	@Select(" SELECT SUM(curr_remaining) as CURR_REMAINING, CURR_CODE, CODE_A3 FROM ( "
			+ "SELECT ccus.curr_remaining,ccus.curr_code , ci.CODE_A3 FROM t_cm_atm_actual_state aas "
			+ "JOIN t_cm_cashout_curr_stat ccus ON ( aas.atm_id = ccus.atm_id "
			+ "AND ccus.encashment_id = aas.cash_out_encashment_id AND ccus.stat_date = aas.cash_out_stat_date) "
			+ "JOIN t_cm_curr ci on (ccus.curr_code = ci.code_n3) "
			+ "WHERE aas.atm_id in (select id from t_cm_temp_atm_list) union "
			+ "SELECT ccus.curr_remaining,ccus.curr_code , ci.CODE_A3 FROM t_cm_atm_actual_state aas "
			+ "JOIN t_cm_cashin_r_curr_stat ccus ON ( aas.atm_id = ccus.atm_id "
			+ "AND ccus.cash_in_encashment_id = aas.cash_in_encashment_id "
			+ "AND ccus.stat_date = aas.cash_out_stat_date) JOIN t_cm_curr ci on (ccus.curr_code = ci.code_n3) "
			+ "WHERE aas.atm_id in (select id from t_cm_temp_atm_list)  ) GROUP BY CURR_CODE, CODE_A3 "
			+ "ORDER BY curr_code")
	@Options(useCache = true, fetchSize = 1000)
	List<Pair> getAtmsRemainingSums();
	
	@ConstructorArgs({
		@Arg(column = "DENOM_COUNT", javaType = String.class),
		@Arg(column = "CODE_A3", javaType = String.class)
	})
	@Select(" SELECT sum(epd.DENOM_VALUE*epd.DENOM_COUNT) as DENOM_COUNT, ci.CODE_A3 "
			+ "FROM T_CM_ENC_CASHOUT_STAT ep "
			+ "join V_CM_ENC_CASHOUT_STAT_DETAILS epd on(ep.ENCASHMENT_ID = epd.ENCASHMENT_ID and epd.ACTION_TYPE = 1) "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE trunc(ep.ENC_DATE) = #{date} "
			+ "AND ep.atm_id in (select id from t_cm_temp_atm_list) AND epd.action_type = #{actionId} "
			+ "GROUP BY ci.CODE_A3 ORDER BY DENOM_COUNT DESC")
	@Options(useCache = true, fetchSize = 1000)
	List<Pair> getAtmsEncStatSums(@Param("date") java.sql.Date date, @Param("actionId") Integer actionId);
}
