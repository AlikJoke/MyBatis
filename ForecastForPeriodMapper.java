package ru.bpc.cm.forecasting.orm;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.session.SqlSession;

import ru.bpc.cm.cashmanagement.orm.builders.ForecastForPeriodBuilder;
import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.forecasting.controllers.ForecastForPeriodController;
import ru.bpc.cm.items.encashments.AtmCurrStatItem;

/**
 * Интерфейс-маппер для класса {@link ForecastForPeriodController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 04.03.2017
 * @version 1.0.0
 *
 */
public interface ForecastForPeriodMapper extends IMapper {

	@Results({
		@Result(column = "code_a3", property = "currCodeA3", javaType = String.class),
		@Result(column = "curr_remaining", property = "crRemainingEndDay", javaType = Long.class),
		@Result(column = "take_off", property = "crSummTakeOff", javaType = Long.class),
		@Result(column = "stat_Date", property = "statDate", javaType = Timestamp.class),
		@Result(column = "RNK", property = "rnk", javaType = Integer.class),
		@Result(column = "CNT", property = "cnt", javaType = Integer.class),
		@Result(column = "CURR_REMAINING", property = "summEncFromAtm", javaType = Long.class)
	})
	@Select("select cs.ATM_ID,cs.STAT_DATE,cc.CODE_A3, "
			+ "cs.CURR_SUMM as TAKE_OFF, cs.CURR_REMAINING as CURR_REMAINING, "
			+ "dense_rank() over(partition by cs.STAT_DATE order by cs.ENCASHMENT_ID) as RNK, "
			+ "count(1) over(partition by cs.STAT_DATE) as CNT from t_cm_cashout_curr_stat cs "
			+ "join t_cm_curr cc on (cc.code_n3 = cs.CURR_CODE) where cs.atm_id = #{atmId} AND cs.stat_Date > #{startDate} "
			+ " AND cs.stat_Date <= #{endDate} AND cs.CURR_CODE = #{currCode} order by cs.stat_Date ")
	@Options(useCache = true, fetchSize = 1000)
	@ResultType(AtmCurrStatItem.class)
	List<AtmCurrStatItem> getCoStatDetailsCurr(@Param("atmId") Integer atmId, @Param("startDate") Date startDate,
			@Param("endDate") Timestamp endDate, @Param("currCode") Integer currCode);
	
	@Results({
		@Result(column = "code_a3", property = "currCodeA3", javaType = String.class),
		@Result(column = "curr_remaining", property = "crRemainingEndDay", javaType = Long.class),
		@Result(column = "take_in", property = "crSummInsert", javaType = Long.class),
		@Result(column = "take_off", property = "crSummTakeOff", javaType = Long.class),
		@Result(column = "stat_Date", property = "statDate", javaType = Timestamp.class),
		@Result(column = "RNK", property = "rnk", javaType = Integer.class),
		@Result(column = "CNT", property = "cnt", javaType = Integer.class),
		@Result(column = "CURR_REMAINING", property = "summEncFromAtm", javaType = Long.class)
	})
	@Select("select cs.ATM_ID,cs.STAT_DATE,cc.CODE_A3, "
			+ "cs.CURR_SUMM_IN as TAKE_IN, cs.CURR_SUMM_OUT as TAKE_OFF, cs.CURR_REMAINING as CURR_REMAINING, "
			+ "dense_rank() over(partition by cs.STAT_DATE order by cs.CASH_IN_ENCASHMENT_ID) as RNK, "
			+ "count(1) over(partition by cs.STAT_DATE) as CNT from t_cm_cashin_r_curr_stat cs "
			+ "join t_cm_curr cc on (cc.code_n3 = cs.CURR_CODE) where cs.atm_id =#{atmId} AND cs.stat_Date > #{startDate} "
			+ " AND cs.stat_Date <= #{endDate} AND cs.CURR_CODE = #{currCode} order by cs.stat_Date ")
	@Options(useCache = true, fetchSize = 1000)
	@ResultType(AtmCurrStatItem.class)
	List<AtmCurrStatItem> getCrStatDetailsCurr(@Param("atmId") Integer atmId, @Param("startDate") Date startDate,
			@Param("endDate") Timestamp endDate, @Param("currCode") Integer currCode);
	
	@Results({
		@Result(column = "curr_remaining", property = "ciRemainingEndDay", javaType = Long.class),
		@Result(column = "take_in", property = "crSummInsert", javaType = Long.class),
		@Result(column = "take_off", property = "ciSummInsert", javaType = Long.class),
		@Result(column = "stat_Date", property = "statDate", javaType = Timestamp.class),
		@Result(column = "RNK", property = "rnk", javaType = Integer.class),
		@Result(column = "CNT", property = "cnt", javaType = Integer.class),
		@Result(column = "CURR_REMAINING", property = "summEncFromAtm", javaType = Long.class)
	})
	@ResultType(AtmCurrStatItem.class)
	@SelectProvider(type = ForecastForPeriodBuilder.class, method = "getStatDetailsCashInBuilder")
	@Options(useCache = true, fetchSize = 1000)
	List<AtmCurrStatItem> getStatDetailsCashIn(@Param("atmId") Integer atmId, @Param("startDate") Date startDate,
			@Param("endDate") Timestamp endDate, @Param("encIds") List<Integer> encIds);

	@Result(column = "CASH_IN_ENCASHMENT_ID", javaType = Integer.class)
	@Select("SELECT DISTINCT CASH_IN_ENCASHMENT_ID FROM T_CM_CASHIN_STAT WHERE STAT_DATE > #{statDate} and ATM_ID = #{atmId}")
	@ResultType(Integer.class)
	List<Integer> getCiEncIdsForPeriod(@Param("atmId") Integer atmId, @Param("statDate") Timestamp statDate);

	@Result(column = "stat_date", javaType = Timestamp.class)
	@Select("select COALESCE(CASH_OUT_STAT_DATE,CASH_IN_STAT_DATE) as stat_date from T_CM_ATM_ACTUAL_STATE "
			+ "where atm_id = #{atmId} ")
	@ResultType(Timestamp.class)
	Timestamp getStatsEnd(@Param("atmId") Integer atmId);

	@Delete("DELETE FROM T_CM_ENC_PERIOD_DENOM WHERE ENC_PERIOD_ID IN (SELECT ID FROM T_CM_ENC_PERIOD WHERE ATM_ID = #{atmId} )")
	void insertPeriodForecastData_deletePeriodDenom(@Param("atmId") Integer atmId);

	@Delete("DELETE FROM T_CM_ENC_PERIOD_CURR WHERE ENC_PERIOD_ID IN (SELECT ID FROM T_CM_ENC_PERIOD WHERE ATM_ID = #{atmId} )")
	void insertPeriodForecastData_deletePeriodCurr(@Param("atmId") Integer atmId);

	@Delete("DELETE FROM T_CM_ENC_PERIOD_STAT WHERE ATM_ID = #{atmId}")
	void insertPeriodForecastData_deletePeriodStat(@Param("atmId") Integer atmId);
	
	@Delete("DELETE FROM T_CM_ENC_PERIOD WHERE ATM_ID = #{atmId}")
	void insertPeriodForecastData_deletePeriod(@Param("atmId") Integer atmId);
	
	@InsertProvider(type = ForecastForPeriodBuilder.class, method = "insertPeriodForecastData_insertPeriod")
	void insertPeriodForecastData_insertPeriod(@Param("nextSeq") String nextSeq, @Param("atmId") Integer atmId,
			@Param("forthcomingEncDate") Timestamp forthcomingEncDate, @Param("encTypeId") Integer encTypeId,
			@Param("forecastResp") Integer forecastResp, @Param("isCashInExists") Boolean isCashInExists,
			@Param("isEmergencyEncashment") Boolean isEmergencyEncashment);

	@Insert("Insert into T_CM_ENC_PERIOD_CURR (ENC_PERIOD_ID,CURR_SUMM,CURR_CODE_A3) VALUES (#{encPlanId}, #{key}, #{label})")
	void insertPeriodForecastData_insertPeriodCurr(@Param("encPlanId") Integer encPlanId, @Param("key") String key,
			@Param("label") String label);

	@Insert("Insert into T_CM_ENC_PERIOD_DENOM (ENC_PERIOD_ID, DENOM_CURR, DENOM_COUNT, DENOM_VALUE) "
			+ " VALUES (#{encPlanId}, #{curr}, #{countInOneCassPlan}, #{denom})")
	void insertPeriodForecastData_insertPeriodDenom(@Param("encPlanId") Integer encPlanId, @Param("curr") Integer curr,
			@Param("countInOneCassPlan") Integer countInOneCassPlan, @Param("denom") Integer denom);

	@Insert("INSERT INTO T_CM_ENC_PERIOD_STAT (ATM_ID,STAT_DATE,CURR_CODE, "
			+ "CO_CURR_SUMM, CO_REMAINING_START_DAY, CO_REMAINING_END_DAY,"
			+ "CURR_LOADED_TO_ATM,CURR_LOADED_FROM_ATM,EMERGENCY_ENCASHMENT,FORECAST,CASH_ADD_ENCASHMENT, "
			+ "CI_CURR_SUMM, CI_REMAINING_START_DAY, CI_REMAINING_END_DAY,"
			+ "CR_CURR_SUMM_IN, CR_CURR_SUMM_OUT, CR_REMAINING_START_DAY, CR_REMAINING_END_DAY) VALUES "
			+ " (#{atmId},#{statDate},#{currNodeN3},#{coSummTakeOff},#{coRemainingStartDay},#{coRemainingEndDay},"
			+ "#{summEncToAtm},#{summEncFromAtm},#{isEmergencyEncashment},#{isForecast},#{isCashAddEncashment},#{ciSummInsert},"
			+ "#{ciRemainingStartDay},#{ciRemainingEndDay},#{crSummInsert},#{crSummTakeOff},#{crRemainingStartDay},#{crRemainingEndDay}) ")
	void insertPeriodForecastData_insertPeriodStat(@Param("atmId") Integer atmId, @Param("statDate") Long statDate,
			@Param("currNodeN3") Integer currNodeN3, @Param("coSummTakeOff") Long coSummTakeOff,
			@Param("coRemainingStartDay") Long coRemainingStartDay, @Param("coRemainingEndDay") Long coRemainingEndDay,
			@Param("summEncToAtm") Long summEncToAtm, @Param("summEncFromAtm") Long summEncFromAtm,
			@Param("isEmergencyEncashment") Boolean isEmergencyEncashment, @Param("isForecast") Boolean isForecast,
			@Param("isCashAddEncashment") Boolean isCashAddEncashment, @Param("ciSummInsert") Long ciSummInsert,
			@Param("ciRemainingStartDay") Long ciRemainingStartDay, @Param("ciRemainingEndDay") Long ciRemainingEndDay,
			@Param("crSummInsert") Long crSummInsert, @Param("crSummTakeOff") Long crSummTakeOff,
			@Param("crRemainingStartDay") Long crRemainingStartDay, @Param("crRemainingEndDay") Long crRemainingEndDay);
	
	@Result(column = "SQ", javaType = Integer.class)
	@SelectProvider(type = ForecastForPeriodBuilder.class, method = "getSQBuilder")
	@ResultType(Integer.class)
	@Options(useCache = true)
	Integer getSQ(@Param("session") SqlSession session);
}
