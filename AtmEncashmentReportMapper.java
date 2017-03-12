package ru.bpc.cm.encashments.orm;

import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.encashments.AtmEncashmentReportController;
import ru.bpc.cm.items.encashments.HourRemaining;

/**
 * Интерфейс-маппер для класса {@link AtmEncashmentReportController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 12.03.2017
 * @version 1.0.0
 *
 */
public interface AtmEncashmentReportMapper extends IMapper {

	@Results({
		@Result(column = "STAT_DATE", property = "statDate", javaType = Timestamp.class),
		@Result(column = "END_OF_STATS_DATE", property = "endOfStatsDate", javaType = Boolean.class),
		@Result(column = "curr_remaining", property = "remaining", javaType = Long.class),
	})
	@Select("SELECT STAT_DATE, CURR_REMAINING, END_OF_STATS_DATE FROM ("
			+ "select ds.stat_date,ds.BILLS_REMAINING as curr_remaining, cash_in_encashment_id, 0 as END_OF_STATS_DATE "
			+ "from T_CM_CASHIN_STAT ds where ds.STAT_DATE < #{statsEndDate} AND ds.STAT_DATE >= startCal AND ds.atm_id = #{atmId} UNION ALL "
			+ "SELECT STAT_DATE, REMAINING as CURR_REMAINING, 0 as cash_in_encashment_id, END_OF_STATS_DATE "
			+ "FROM t_cm_temp_enc_report) order by stat_date,cash_in_encashment_id ")
	@ResultType(HourRemaining.class)
	@Options(useCache = true, fetchSize = 1000)
	List<HourRemaining> getCashInStatRemain(@Param("statsEndDate") Timestamp statsEndDate,
			@Param("startCal") Timestamp startCal, @Param("atmId") Integer atmId);
	
	@ConstructorArgs({
		@Arg(column = "CURR_CODE", javaType = String.class),
		@Arg(column = "curr_remaining", javaType = Long.class),
		@Arg(column = "STAT_DATE", javaType = Timestamp.class),
		@Arg(column = "END_OF_STATS_DATE", javaType = Boolean.class)
	})
	@Select("SELECT STAT_DATE, CURR_REMAINING, CURR_CODE, END_OF_STATS_DATE FROM ( "
			+ "select ds.stat_date,ds.CURR_REMAINING as curr_remaining, ci.CODE_A3 as CURR_CODE, encashment_id, 0 as END_OF_STATS_DATE "
			+ "from T_CM_CASHOUT_CURR_STAT ds join T_CM_CURR ci on (ci.CODE_N3 = ds.CURR_CODE) "
			+ "where ds.STAT_DATE < #{statsEndDate} AND ds.STAT_DATE >= #{startCal} AND ds.atm_id = #{atmId} UNION ALL "
			+ "SELECT STAT_DATE, REMAINING as CURR_REMAINING, CURR_CODE, 0 as  encashment_id, END_OF_STATS_DATE "
			+ "FROM t_cm_temp_enc_report) order by curr_code, stat_date, encashment_id ")
	@ResultType(HourRemaining.class)
	@Options(useCache = true, fetchSize = 1000)
	List<HourRemaining> getCashOutStatRemain(@Param("statsEndDate") Timestamp statsEndDate,
			@Param("startCal") Timestamp startCal, @Param("atmId") Integer atmId);
	
	@ConstructorArgs({
		@Arg(column = "CURR_CODE", javaType = String.class),
		@Arg(column = "curr_remaining", javaType = Long.class),
		@Arg(column = "STAT_DATE", javaType = Timestamp.class),
		@Arg(column = "END_OF_STATS_DATE", javaType = Boolean.class)
	})
	@Select("SELECT STAT_DATE, CURR_REMAINING, CURR_CODE, END_OF_STATS_DATE FROM ( "
			+ "select ds.stat_date,ds.CURR_REMAINING as curr_remaining, ci.CODE_A3 as CURR_CODE, cash_in_encashment_id, 0 as END_OF_STATS_DATE "
			+ "from T_CM_CASHIN_R_CURR_STAT ds join T_CM_CURR ci on (ci.CODE_N3 = ds.CURR_CODE) "
			+ "where ds.STAT_DATE < #{statsEndDate} AND ds.STAT_DATE >= #{startCal} AND ds.atm_id = #{atmId} UNION ALL "
			+ "SELECT STAT_DATE, REMAINING as CURR_REMAINING, CURR_CODE, 0 as  cash_in_encashment_id, END_OF_STATS_DATE "
			+ "FROM t_cm_temp_enc_report) order by CURR_CODE, stat_date, cash_in_encashment_id ")
	@ResultType(HourRemaining.class)
	@Options(useCache = true, fetchSize = 1000)
	List<HourRemaining> getCashRecCurrStatRemain(@Param("statsEndDate") Timestamp statsEndDate,
			@Param("startCal") Timestamp startCal, @Param("atmId") Integer atmId);
	
	@ConstructorArgs({
		@Arg(column = "CURR_CODE", javaType = String.class),
		@Arg(column = "curr_remaining", javaType = Long.class),
		@Arg(column = "STAT_DATE", javaType = Timestamp.class),
		@Arg(column = "END_OF_STATS_DATE", javaType = Boolean.class)
	})
	@Select("SELECT STAT_DATE, CURR_REMAINING, CURR_CODE,END_OF_STATS_DATE FROM ( "
			+ "select ds.stat_date,sum(ds.CASS_REMAINING) as curr_remaining, ci.CODE_A3 as CURR_CODE, "
			+ "cash_in_encashment_id, 0 as END_OF_STATS_DATE from T_CM_CASHIN_R_CASS_STAT ds "
			+ "join T_CM_CURR ci on (ci.CODE_N3 = ds.CASS_CURR) where ds.STAT_DATE < #{statsEndDate} AND ds.STAT_DATE >= #{startCal} "
			+ "AND ds.atm_id = #{atmId} group by ds.stat_date,ds.cash_in_encashment_id,ci.CODE_A3 UNION ALL "
			+ "SELECT STAT_DATE, REMAINING as CURR_REMAINING, CURR_CODE, "
			+ "0 as  cash_in_encashment_id, END_OF_STATS_DATE FROM t_cm_temp_enc_report) "
			+ "order by CURR_CODE,stat_date,cash_in_encashment_id ")
	@ResultType(HourRemaining.class)
	@Options(useCache = true, fetchSize = 1000)
	List<HourRemaining> getCashRecBillsStatRemain(@Param("statsEndDate") Timestamp statsEndDate,
			@Param("startCal") Timestamp startCal, @Param("atmId") Integer atmId);
}
