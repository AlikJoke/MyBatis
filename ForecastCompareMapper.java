package ru.bpc.cm.forecasting.orm;

import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Flush;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.executor.BatchResult;

import ru.bpc.cm.cashmanagement.orm.builders.ForecastCompareBuilder;
import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.forecasting.controllers.ForecastCompareController;

/**
 * Интерфейс-маппер для класса {@link ForecastCompareController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 23.02.2017
 * @version 1.0.0
 *
 */
public interface ForecastCompareMapper extends IMapper {

	@Delete("DELETE FROM T_CM_ENC_COMPARE_DENOM WHERE ENC_COMPARE_ID IN (SELECT ID FROM T_CM_ENC_COMPARE WHERE ATM_ID = #{atmId} )")
	void deleteCompareDenomData(@Param("atmId") Integer atmId);

	@Delete("DELETE FROM T_CM_ENC_COMPARE_STAT WHERE ATM_ID = #{atmId}")
	void deleteCompareStatData(@Param("atmId") Integer atmId);

	@Delete("DELETE FROM T_CM_ENC_COMPARE WHERE ATM_ID = #{atmId}")
	void deleteCompareData(@Param("atmId") Integer atmId);

	@InsertProvider(type = ForecastCompareBuilder.class, method = "insertCompareDataBuilder")
	void insertCompareData(@Param("nextSeq") String nextSeq, @Param("atmId") Integer atmId,
			@Param("encDate") Timestamp encDate, @Param("encType") Integer encType, @Param("resp") Integer resp,
			@Param("isExists") Boolean isExists, @Param("isEmergency") Boolean isEmergency,
			@Param("encLosts") Long encLosts, @Param("encPrice") Long encPrice,
			@Param("encLostsCurrCode") Integer encLostsCurrCode);

	@Result(column = "SQ", javaType = Integer.class)
	@SelectProvider(type = ForecastCompareBuilder.class, method = "getPlanIdBuilder")
	Integer getPlanId(@Param("currSeq") String currSeq, @Param("from") String from);

	@Insert("Insert into T_CM_ENC_COMPARE_DENOM " + " (ENC_COMPARE_ID, DENOM_CURR, DENOM_COUNT, DENOM_VALUE) "
			+ " VALUES " + " (#{encPlanId}, #{curr}, #{countInOneCassPlan}, #{denom})")
	void insertCompareDenomData(@Param("encPlanId") Integer encPlanId, @Param("curr") Integer curr,
			@Param("countInOneCassPlan") Integer countInOneCassPlan, @Param("denom") Integer denom);

	@Insert("INSERT INTO T_CM_ENC_COMPARE_STAT (ATM_ID,STAT_DATE,CURR_CODE, "
			+ "CO_CURR_SUMM, CO_REMAINING_START_DAY, CO_REMAINING_END_DAY,"
			+ "CURR_LOADED_TO_ATM,CURR_LOADED_FROM_ATM,EMERGENCY_ENCASHMENT,FORECAST,CASH_ADD_ENCASHMENT, "
			+ "CI_CURR_SUMM, CI_REMAINING_START_DAY, CI_REMAINING_END_DAY,"
			+ "CR_CURR_SUMM_IN, CR_CURR_SUMM_OUT, CR_REMAINING_START_DAY, CR_REMAINING_END_DAY ) VALUES "
			+ " (#{atmId},#{statDate},#{currCodeN3},#{coSummTakeOff},#{coRemainingStartDay},#{coRemainingEndDay},"
			+ "#{summEncToAtm},#{summEncFromAtm},#{isEmergencyEncashment},#{isForecast},#{isCashAddEncashment},"
			+ "#{ciSummInsert},#{ciRemainingStartDay},#{ciRemainingEndDay},#{crSummInsert},#{crSummTakeOff},#{crRemainingStartDay},#{crRemainingEndDay}) ")
	void insertCompareForecastData(@Param("atmId") Integer atmId, @Param("statDate") Timestamp statDate,
			@Param("currCodeN3") Integer currCodeN3, @Param("coSummTakeOff") Long coSummTakeOff,
			@Param("coRemainingStartDay") Long coRemainingStartDay, @Param("coRemainingEndDay") Long coRemainingEndDay,
			@Param("summEncToAtm") Long summEncToAtm, @Param("summEncFromAtm") Long summEncFromAtm,
			@Param("isEmergencyEncashment") Boolean isEmergencyEncashment, @Param("isForecast") Boolean isForecast,
			@Param("isCashAddEncashment") Boolean isCashAddEncashment, @Param("ciSummInsert") Long ciSummInsert,
			@Param("ciRemainingStartDay") Long ciRemainingStartDay, @Param("ciRemainingEndDay") Long ciRemainingEndDay,
			@Param("crSummInsert") Long crSummInsert, @Param("crSummTakeOff") Long crSummTakeOff,
			@Param("crRemainingStartDay") Long crRemainingStartDay, @Param("crRemainingEndDay") Long crRemainingEndDay);

	@Select("SELECT COUNT(DISTINCT stat_date) AS cnt FROM t_cm_cashout_curr_stat WHERE atm_id = #{atmId} "
			+ "AND stat_date > #{startDate} AND stat_date < #{endDate} ")
	Integer getStatsDatesCount(@Param("atmId") Integer atmId, @Param("startDate") Timestamp startDate,
			@Param("endDate") Timestamp endDate);

	@Flush
	List<BatchResult> flush();
}
