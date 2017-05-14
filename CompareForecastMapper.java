package ru.bpc.cm.optimization.orm;

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

import ru.bpc.cm.items.encashments.AtmPeriodEncashmentItem;
import ru.bpc.cm.items.encashments.EncashmentCassItem;
import ru.bpc.cm.orm.common.IMapper;
import ru.bpc.cm.orm.items.TripleObject;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.Pair;

/**
 * Интерфейс-маппер для класса CompareForecastController.
 * 
 * @author Alimurad A. Ramazanov
 * @since 29.04.2017
 * @version 1.0.0
 *
 */
public interface CompareForecastMapper extends IMapper {

	@Result(column = "cnt", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT count(ecs.id) AS cnt FROM t_cm_enc_compare ecs WHERE  ecs.atm_id = #{atmId} "
			+ "AND ecs.date_forthcoming_encashment > #{startDate} AND ecs.date_forthcoming_encashment < #{endDate} ")
	Integer getEncCount(@Param("atmId") Integer atmId, @Param("startDate") Timestamp startDate,
			@Param("endDate") Timestamp endDate);

	@Results({
			@Result(column = "enc_losts_curr", property = "key", javaType = Integer.class),
			@Result(column = "enc_price", property = "value", javaType = Long.class) 
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT sum(ecs.ENC_PRICE) AS enc_price,enc_losts_curr FROM t_cm_enc_compare ecs WHERE "
			+ " ecs.atm_id = #{atmId} AND ecs.date_forthcoming_encashment > #{startDate} "
			+ "AND ecs.date_forthcoming_encashment < #{endDate} GROUP BY enc_losts_curr")
	List<ObjectPair<Integer, Long>> getEncPriceWithCurr(@Param("atmId") Integer atmId,
			@Param("startDate") Timestamp startDate, @Param("endDate") Timestamp endDate);
	
	@Results({
			@Result(column = "stat_date", property = "first", javaType = Timestamp.class),
			@Result(column = "curr_loaded_to_atm", property = "second", javaType = Double.class),
			@Result(column = "curr_loaded_from_atm", property = "third", javaType = Double.class)
	})
	@ResultType(TripleObject.class)
	@Select("SELECT atm_id,stat_date,co_remaining_start_day AS curr_loaded_to_atm, 0 AS curr_loaded_from_Atm "
			+ "FROM t_cm_enc_compare_stat WHERE atm_id = #{atmId} AND stat_date = #{startDate} AND curr_code = #{currCode} "
			+ "UNION SELECT atm_id, stat_date,curr_loaded_to_atm,curr_loaded_from_atm*(1-CASH_ADD_ENCASHMENT) "
			+ "FROM t_cm_enc_compare_stat WHERE (curr_loaded_to_atm > 0 OR curr_loaded_from_Atm > 0) "
			+ "AND atm_id = #{atmId} AND curr_code = #{currCode} AND stat_date >= #{startDate} AND stat_Date <= #{endDate} UNION "
			+ "SELECT atm_id,stat_date,0 AS curr_loaded_to_atm, co_remaining_end_day AS curr_loaded_from_Atm "
			+ "FROM t_cm_enc_compare_stat WHERE atm_id = #{atmId} AND stat_date = #{endDate} AND curr_code = #{currCode} "
			+ "ORDER BY stat_date ")
	@Options(useCache = true, fetchSize = 1000)
	List<TripleObject<Timestamp, Double, Double>> getEncLostsForCurr(@Param("atmId") Integer atmId,
			@Param("startDate") Timestamp startDate, @Param("endDate") Timestamp endDate,
			@Param("currCode") Integer currCode);

	@Results({
			@Result(column = "enc_losts_curr", property = "key", javaType = Integer.class),
			@Result(column = "enc_losts", property = "value", javaType = Long.class) 
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT enc_losts, enc_losts_curr FROM t_cm_enc_compare ecs WHERE  ecs.atm_id = #{atmId} "
			+ "AND ecs.date_forthcoming_encashment >= #{startDate} AND ecs.date_forthcoming_encashment <= #{endDate} "
			+ "order by date_forthcoming_encashment desc")
	@Options(useCache = true, fetchSize = 1000)
	List<ObjectPair<Integer, Long>> getEncLostsForLastEnc(@Param("atmId") Integer atmId,
			@Param("startDate") Timestamp startDate, @Param("endDate") Timestamp endDate);
	
	@ConstructorArgs({
			@Arg(column = "EMERGENCY_ENCASHMENT", javaType = Boolean.class),
			@Arg(column = "DATE_FORTHCOMING_ENCASHMENT", javaType = Timestamp.class),
			@Arg(column = "FORECAST_RESP_CODE", javaType = Integer.class),
			@Arg(column = "ID", javaType = Integer.class),
			@Arg(column = "ENCASHMENT_TYPE", javaType = Integer.class),
	})
	@ResultType(AtmPeriodEncashmentItem.class)
	@Select("SELECT ID,DATE_FORTHCOMING_ENCASHMENT,ENCASHMENT_TYPE,FORECAST_RESP_CODE,"
			+ "CASH_IN_EXISTS,EMERGENCY_ENCASHMENT FROM T_CM_ENC_COMPARE WHERE ATM_ID = #{atmId} "
			+ "AND DATE_FORTHCOMING_ENCASHMENT <= #{endDate} AND DATE_FORTHCOMING_ENCASHMENT >= #{startDate} ")
	@Options(useCache = true, fetchSize = 1000)
	List<AtmPeriodEncashmentItem> getEncListForecast(@Param("atmId") Integer atmId,
			@Param("startDate") Timestamp startDate, @Param("endDate") Timestamp endDate);
	
	@ConstructorArgs({
			@Arg(column = "CURR_SUMM", javaType = String.class),
			@Arg(column = "CURR_CODE_A3", javaType = String.class)
	})
	@ResultType(Pair.class)
	@Select("SELECT CODE_A3 as CURR_CODE_A3, SUM(DENOM_COUNT*DENOM_VALUE) as CURR_SUMM "
			+ "FROM T_CM_ENC_COMPARE_DENOM join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) "
			+ "WHERE ENC_COMPARE_ID = #{encPeriodId} GROUP BY CODE_A3 ORDER BY CURR_SUMM DESC")
	@Options(useCache = true, fetchSize = 1000)
	List<Pair> getEncCurrs(@Param("encPeriodId") Integer encPeriodId);
	
	@Results({
			@Result(column = "DENOM_COUNT", property = "denomCount", javaType = Integer.class),
			@Result(column = "DENOM_VALUE", property = "denomValue", javaType = Integer.class),
			@Result(column = "DENOM_CURR", property = "denomCurr", javaType = Integer.class),
			@Result(column = "CODE_A3", property = "denomCurrA3", javaType = String.class)
	})
	@ResultType(EncashmentCassItem.class)
	@Select("SELECT DENOM_VALUE,DENOM_COUNT, DENOM_CURR , CODE_A3 FROM T_CM_ENC_COMPARE_DENOM "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE ENC_COMPARE_ID = #{encPeriodId} "
			+ "ORDER BY DENOM_CURR,DENOM_VALUE DESC")
	@Options(useCache = true, fetchSize = 1000)
	List<EncashmentCassItem> getEncDenoms(@Param("encPeriodId") Integer encPeriodId);
}
