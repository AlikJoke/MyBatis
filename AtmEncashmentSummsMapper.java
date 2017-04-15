package ru.bpc.cm.encashments.orm;

import java.sql.Date;
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
import ru.bpc.cm.encashments.AtmEncashmentSummsController;
import ru.bpc.cm.items.encashments.EncashmentCassItem;
import ru.bpc.cm.utils.Pair;

/**
 * Интерфейс-маппер для класса {@link AtmEncashmentSummsController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 12.03.2017
 * @version 1.0.0
 *
 */
public interface AtmEncashmentSummsMapper extends IMapper {

	@Result(column = "ENC_COUNT", javaType = Integer.class)
	@Select("SELECT count(distinct ep.ENC_PLAN_ID) as ENC_COUNT FROM T_CM_ENC_PLAN ep "
			+ "join T_CM_ENC_PLAN_DENOM epd on (ep.ENC_PLAN_ID = epd.ENC_PLAN_ID) WHERE "
			+ "trunc(ep.DATE_FORTHCOMING_ENCASHMENT) = #{date} AND ep.atm_id in (select id from t_cm_temp_atm_list) ")
	@ResultType(Integer.class)
	Integer getEncsCount(@Param("date") Date date);

	@Results({ @Result(column = "DENOM_COUNT", property = "denomCount", javaType = Integer.class),
			@Result(column = "DENOM_VALUE", property = "denomValue", javaType = Integer.class),
			@Result(column = "DENOM_CURR", property = "denomCurr", javaType = Integer.class),
			@Result(column = "CODE_A3", property = "denomCurrA3", javaType = String.class) })
	@Select("SELECT epd.DENOM_VALUE, sum(epd.DENOM_COUNT) as DENOM_COUNT, epd.DENOM_CURR , ci.CODE_A3 "
			+ "FROM T_CM_ENC_PLAN ep join T_CM_ENC_PLAN_DENOM epd on(ep.ENC_PLAN_ID = epd.ENC_PLAN_ID) "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE "
			+ "trunc(ep.DATE_FORTHCOMING_ENCASHMENT) = #{date} AND ep.atm_id in (select id from t_cm_temp_atm_list) "
			+ "GROUP BY epd.DENOM_VALUE,epd.DENOM_CURR,ci.CODE_A3 ORDER BY DENOM_CURR,DENOM_VALUE DESC")
	@ResultType(EncashmentCassItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<EncashmentCassItem> getEncsDenoms(@Param("date") Date date);

	@ConstructorArgs({ @Arg(column = "DENOM_COUNT", javaType = String.class),
			@Arg(column = "CODE_A3", javaType = String.class) })
	@Select("SELECT sum(epd.DENOM_VALUE*epd.DENOM_COUNT) as DENOM_COUNT, epd.DENOM_CURR , ci.CODE_A3 "
			+ "FROM T_CM_ENC_PLAN ep " + "join T_CM_ENC_PLAN_DENOM epd on(ep.ENC_PLAN_ID = epd.ENC_PLAN_ID) "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE "
			+ "trunc(ep.DATE_FORTHCOMING_ENCASHMENT) = #{date} AND ep.atm_id in (select id from t_cm_temp_atm_list) "
			+ "GROUP BY epd.DENOM_CURR,ci.CODE_A3 ORDER BY DENOM_CURR")
	@ResultType(Pair.class)
	@Options(useCache = true, fetchSize = 1000)
	List<Pair> getEncsCurrs(@Param("date") Date date);

	@Results({ @Result(column = "DENOM_COUNT", property = "denomCount", javaType = Integer.class),
			@Result(column = "DENOM_VALUE", property = "denomValue", javaType = Integer.class),
			@Result(column = "DENOM_CURR", property = "denomCurr", javaType = Integer.class),
			@Result(column = "CODE_A3", property = "denomCurrA3", javaType = String.class) })
	@Select("SELECT epd.DENOM_VALUE, sum(epd.DENOM_COUNT) as DENOM_COUNT, epd.DENOM_CURR , ci.CODE_A3 "
			+ "FROM T_CM_ENC_PLAN ep join T_CM_ENC_PLAN_DENOM epd on(ep.ENC_PLAN_ID = epd.ENC_PLAN_ID) "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE "
			+ "trunc(ep.DATE_FORTHCOMING_ENCASHMENT) = #{date} AND ep.ATM_ID = #{atmId} "
			+ "GROUP BY epd.DENOM_VALUE,epd.DENOM_CURR,ci.CODE_A3 ORDER BY DENOM_CURR,DENOM_VALUE DESC")
	@ResultType(EncashmentCassItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<EncashmentCassItem> getEncsDenomsById(@Param("date") Date date, @Param("atmId") Integer atmId);

	@ConstructorArgs({ @Arg(column = "DENOM_COUNT", javaType = String.class),
			@Arg(column = "CODE_A3", javaType = String.class) })
	@Select("SELECT sum(epd.DENOM_VALUE*epd.DENOM_COUNT) as DENOM_COUNT, epd.DENOM_CURR , ci.CODE_A3 "
			+ "FROM T_CM_ENC_PLAN ep join T_CM_ENC_PLAN_DENOM epd on(ep.ENC_PLAN_ID = epd.ENC_PLAN_ID) "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE "
			+ "trunc(ep.DATE_FORTHCOMING_ENCASHMENT) = #{date} AND ep.ATM_ID = #{atmId} "
			+ "GROUP BY epd.DENOM_CURR,ci.CODE_A3 ORDER BY DENOM_CURR")
	@ResultType(Pair.class)
	@Options(useCache = true, fetchSize = 1000)
	List<Pair> getEncsCurrsById(@Param("date") Date date, @Param("atmId") Integer atmId);

	@Result(column = "ENC_COUNT", javaType = Integer.class)
	@Select("SELECT count(distinct ep.ID) as ENC_COUNT FROM T_CM_ENC_PERIOD ep "
			+ "join T_CM_ENC_PERIOD_DENOM epd on (ep.ID = epd.ENC_PERIOD_ID) WHERE "
			+ "trunc(ep.DATE_FORTHCOMING_ENCASHMENT) = #{date} "
			+ "AND ep.atm_id in (select id from t_cm_temp_atm_list) ")
	@ResultType(Integer.class)
	Integer getEncsCountForPeriod(@Param("date") Date date);

	@Results({ @Result(column = "DENOM_COUNT", property = "denomCount", javaType = Integer.class),
			@Result(column = "DENOM_VALUE", property = "denomValue", javaType = Integer.class),
			@Result(column = "DENOM_CURR", property = "denomCurr", javaType = Integer.class),
			@Result(column = "CODE_A3", property = "denomCurrA3", javaType = String.class) })
	@ResultType(EncashmentCassItem.class)
	@Options(useCache = true, fetchSize = 1000)
	@Select("SELECT epd.DENOM_VALUE, sum(epd.DENOM_COUNT) as DENOM_COUNT, epd.DENOM_CURR , ci.CODE_A3 "
			+ "FROM T_CM_ENC_PERIOD ep join T_CM_ENC_PERIOD_DENOM epd on(ep.ID = epd.ENC_PERIOD_ID) "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE "
			+ "trunc(ep.DATE_FORTHCOMING_ENCASHMENT) = #{date} AND ep.atm_id in (select id from t_cm_temp_atm_list) "
			+ "GROUP BY epd.DENOM_VALUE,epd.DENOM_CURR,ci.CODE_A3 ORDER BY DENOM_CURR,DENOM_VALUE DESC")
	List<EncashmentCassItem> getEncsDenomsForPeriod(@Param("date") Date date);

	@ConstructorArgs({ @Arg(column = "DENOM_COUNT", javaType = String.class),
			@Arg(column = "CODE_A3", javaType = String.class) })
	@Select("SELECT sum(epd.DENOM_VALUE*epd.DENOM_COUNT) as DENOM_COUNT, epd.DENOM_CURR , ci.CODE_A3 "
			+ "FROM T_CM_ENC_PERIOD ep join T_CM_ENC_PERIOD_DENOM epd on(ep.ID = epd.ENC_PERIOD_ID) "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE "
			+ "trunc(ep.DATE_FORTHCOMING_ENCASHMENT) = #{date} "
			+ "AND ep.atm_id in (select id from t_cm_temp_atm_list) GROUP BY epd.DENOM_CURR,ci.CODE_A3 "
			+ "ORDER BY DENOM_CURR")
	@ResultType(Pair.class)
	@Options(useCache = true, fetchSize = 1000)
	List<Pair> getEncsCurrsForPeriod(@Param("date") Date date);

	@Results({ @Result(column = "DENOM_COUNT", property = "denomCount", javaType = Integer.class),
			@Result(column = "DENOM_VALUE", property = "denomValue", javaType = Integer.class),
			@Result(column = "DENOM_CURR", property = "denomCurr", javaType = Integer.class),
			@Result(column = "CODE_A3", property = "denomCurrA3", javaType = String.class) })
	@Select("SELECT epd.DENOM_VALUE, sum(epd.DENOM_COUNT) as DENOM_COUNT, epd.DENOM_CURR , ci.CODE_A3 "
			+ "FROM T_CM_ENC_PERIOD ep join T_CM_ENC_PERIOD_DENOM epd on(ep.ID = epd.ENC_PERIOD_ID) "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE "
			+ "trunc(ep.DATE_FORTHCOMING_ENCASHMENT) = #{date} AND ep.ATM_ID = #{atmId} "
			+ "GROUP BY epd.DENOM_VALUE,epd.DENOM_CURR,ci.CODE_A3 ORDER BY DENOM_CURR,DENOM_VALUE DESC")
	@ResultType(EncashmentCassItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<EncashmentCassItem> getEncsDenomsForPeriodById(@Param("date") Date date, @Param("atmId") Integer atmId);

	@ConstructorArgs({ @Arg(column = "DENOM_COUNT", javaType = String.class),
			@Arg(column = "CODE_A3", javaType = String.class) })
	@Select("SELECT sum(epd.DENOM_VALUE*epd.DENOM_COUNT) as DENOM_COUNT, epd.DENOM_CURR , ci.CODE_A3 "
			+ "FROM T_CM_ENC_PERIOD ep join T_CM_ENC_PERIOD_DENOM epd on(ep.ID = epd.ENC_PERIOD_ID) "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE "
			+ "trunc(ep.DATE_FORTHCOMING_ENCASHMENT) = #{date} AND ep.ATM_ID = #{atmId} "
			+ "GROUP BY epd.DENOM_CURR,ci.CODE_A3 ORDER BY DENOM_CURR")
	@ResultType(Pair.class)
	@Options(useCache = true, fetchSize = 1000)
	List<Pair> getEncsCurrsForPeriodById(@Param("date") Date date, @Param("atmId") Integer atmId);
}
