package ru.bpc.cm.optimization.orm;

import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import ru.bpc.cm.items.optimization.ForecastCompareCurrStat;
import ru.bpc.cm.orm.common.IMapper;
import ru.bpc.cm.utils.ObjectPair;

/**
 * Интерфейс-маппер для класса ForecastStatsCompareController.
 * 
 * @author Alimurad A. Ramazanov
 * @since 29.04.2017
 * @version 1.0.0
 *
 */
public interface ForecastStatsCompareMapper extends IMapper {

	@Results({
			@Result(column = "STAT_DATE", property = "statDate", javaType = Timestamp.class),
			@Result(column = "REMAINING_FORECAST", property = "remainingForecast", javaType = Long.class),
			@Result(column = "REMAINING_STATS", property = "remainingStats", javaType = Long.class)
	})
	@Select("SELECT ecs.STAT_DATE, ecs.CO_REMAINING_END_DAY as REMAINING_FORECAST, "
			+ "eccs.CURR_REMAINING as REMAINING_STATS FROM T_CM_ENC_COMPARE_STAT ecs "
			+ "left outer join t_cm_cashout_curr_stat eccs on (eccs.ATM_ID = ecs.ATM_ID and "
			+ "eccs.stat_date = ecs.stat_date and eccs.curr_code = ecs.curr_code) WHERE ecs.ATM_ID = #{atmId} "
			+ "AND ecs.CURR_CODE = #{curr} AND ecs.STAT_DATE <= #{endDate} AND ecs.STAT_DATE >= #{startDate} ORDER BY STAT_DATE ")
	@ResultType(ForecastCompareCurrStat.class)
	@Options(useCache = true, fetchSize = 1000)
	List<ForecastCompareCurrStat> getCurrRemainings(@Param("atmId") Integer atmId, @Param("curr") Integer curr,
			@Param("startDate") Timestamp startDate, @Param("endDate") Timestamp endDate);
	
	@Results({
			@Result(column = "start_date", property = "key", javaType = Timestamp.class),
			@Result(column = "end_date", property = "value", javaType = Timestamp.class)
	})
	@Select("WITH stats AS (SELECT COUNT(distinct atm_id) AS cnt,stat_date FROM t_cm_enc_compare_stat "
			+ "WHERE atm_id in (select id from t_cm_temp_atm_list) GROUP BY stat_date) "
			+ "SELECT MIN(stat_date) AS start_date, MAX(stat_date) AS end_date FROM stats "
			+ "WHERE cnt = (SELECT COUNT(distinct atm_id) FROM t_cm_enc_compare_stat "
			+ "WHERE atm_id in (select id from t_cm_temp_atm_list))")
	@ResultType(ObjectPair.class)
	@Options(useCache = true, fetchSize = 1000)
	List<ObjectPair<Timestamp, Timestamp>> getCalendarAvailableDays();

	@Result(column = "CNT", javaType = Integer.class)
	@Select("SELECT COUNT(distinct stat_date) AS cnt FROM t_cm_enc_compare_stat WHERE atm_id = #{atmId} "
			+ "AND stat_date >= #{startDate} AND stat_date <= #{endDate} ")
	@ResultType(Integer.class)
	Integer checkForecastPerformedForAtm(@Param("atmId") Integer atmId, @Param("startDate") Timestamp startDate,
			@Param("endDate") Timestamp endDate);
}
