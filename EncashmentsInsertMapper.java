package ru.bpc.cm.forecasting.orm;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;

import ru.bpc.cm.cashmanagement.orm.builders.EncashmentsInsertBuilder;
import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.forecasting.controllers.EncashmentsInsertController;
import ru.bpc.cm.utils.ObjectPair;

/**
 * Интерфейс-маппер для класса {@link EncashmentsInsertController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 04.03.2017
 * @version 1.0.0
 *
 */
public interface EncashmentsInsertMapper extends IMapper {

	@Insert("Insert into T_CM_ENC_PLAN "
			+ " (ENC_PLAN_ID, ATM_ID, DATE_PREVIOUS_ENCASHMENT, INTERVAL_ENC_LAST_TO_FORTH, DATE_FORTHCOMING_ENCASHMENT,  "
			+ " INTERVAL_ENC_FORTH_TO_FUTURE, DATE_FUTURE_ENCASHMENT, "
			+ "  IS_APPROVED, ENC_LOSTS_CURR_CODE, FORECAST_RESP_CODE, "
			+ "EMERGENCY_ENCASHMENT, ENC_LOSTS, ENC_PRICE, CASH_ADD_ENCASHMENT, ENCASHMENT_TYPE,"
			+ "ENC_LOSTS_JOINT, ENC_LOSTS_SPLIT, ENCASHMENT_TYPE_BY_LOSTS, "
			+ "ENC_PRICE_CASH_IN , ENC_PRICE_CASH_OUT, ENC_PRICE_BOTH_IN_OUT) " + " VALUES "
			+ " (#{encPlanId}, #{atmId}, #{lastEncDate}, #{forthcomingEncInterval}, "
			+ "#{forthcomingEncDate}, #{futureEncInterval}, #{futureEncDate}, "
			+ "#{isApproved}, #{lostsCurr}, #{forecastResp}, #{isEmergencyEncashment}, "
			+ "#{encLosts}, #{encPrice}, #{isAddCashmanagement}, #{encTypeId}, #{lostsJointEcnashment}, "
			+ "#{lostsSplitEcnashment}, #{encTypeByLostsId}, #{encPriceCashIn}, #{encPriceCashOut}, #{encPriceBothInOut})")
	void insertForecastData(@Param("encPlanId") Integer encPlanId, @Param("atmId") Integer atmId,
			@Param("lastEncDate") Date lastEncDate, @Param("forthcomingEncInterval") Integer forthcomingEncInterval,
			@Param("forthcomingEncDate") Timestamp forthcomingEncDate,
			@Param("futureEncInterval") Integer futureEncInterval, @Param("futureEncDate") Date futureEncDate,
			@Param("isApproved") Integer isApproved, @Param("lostsCurr") Integer lostsCurr,
			@Param("forecastResp") Integer forecastResp, @Param("isEmergencyEncashment") Boolean isEmergencyEncashment,
			@Param("encLosts") Double encLosts, @Param("encPrice") Double encPrice,
			@Param("isAddCashmanagement") Boolean isAddCashmanagement, @Param("encTypeId") Integer encTypeId,
			@Param("lostsJointEcnashment") Double lostsJointEcnashment,
			@Param("lostsSplitEcnashment") Double lostsSplitEcnashment,
			@Param("encTypeByLostsId") Integer encTypeByLostsId, @Param("encPriceCashIn") Double encPriceCashIn,
			@Param("encPriceCashOut") Double encPriceCashOut, @Param("encPriceBothInOut") Double encPriceBothInOut);

	@Update("UPDATE T_CM_ENC_PLAN SET ATM_ID = #{atmId}, DATE_PREVIOUS_ENCASHMENT = #{lastEncDate}, "
			+ "INTERVAL_ENC_LAST_TO_FORTH = #{forthcomingEncInterval}, "
			+ "DATE_FORTHCOMING_ENCASHMENT = #{forthcomingEncDate}, "
			+ "INTERVAL_ENC_FORTH_TO_FUTURE = #{futureEncInterval}, DATE_FUTURE_ENCASHMENT = #{futureEncDate} , "
			+ "IS_APPROVED = #{isApproved}, ENC_LOSTS_CURR_CODE = #{lostsCurr}, "
			+ "FORECAST_RESP_CODE = #{forecastResp}, EMERGENCY_ENCASHMENT = #{isEmergencyEncashment}, "
			+ "ENC_LOSTS = #{encLosts} , ENC_PRICE = #{encPrice} , "
			+ "CASH_ADD_ENCASHMENT = #{isAddCashmanagement}, ENCASHMENT_TYPE = #{encTypeId}, "
			+ "ENC_LOSTS_JOINT = #{lostsJointEcnashment}, ENC_LOSTS_SPLIT = #{lostsSplitEcnashment}, "
			+ "ENCASHMENT_TYPE_BY_LOSTS = #{encTypeByLostsId}, ENC_PRICE_CASH_IN = #{encPriceCashIn}, "
			+ "ENC_PRICE_CASH_OUT = #{encPriceCashOut}, ENC_PRICE_BOTH_IN_OUT = #{encPriceBothInOut} "
			+ "WHERE ENC_PLAN_ID = #{encPlanId}")
	void updateForecastData(@Param("encPlanId") Integer encPlanId, @Param("atmId") Integer atmId,
			@Param("lastEncDate") Date lastEncDate, @Param("forthcomingEncInterval") Integer forthcomingEncInterval,
			@Param("forthcomingEncDate") Timestamp forthcomingEncDate, @Param("futureEncDate") Date futureEncDate,
			@Param("futureEncInterval") Integer futureEncInterval, @Param("isApproved") Integer isApproved,
			@Param("lostsCurr") Integer lostsCurr, @Param("forecastResp") Integer forecastResp,
			@Param("isEmergencyEncashment") Boolean isEmergencyEncashment, @Param("encLosts") Double encLosts,
			@Param("encPrice") Double encPrice, @Param("isAddCashmanagement") Boolean isAddCashmanagement,
			@Param("encTypeId") Integer encTypeId, @Param("lostsJointEcnashment") Double lostsJointEcnashment,
			@Param("lostsSplitEcnashment") Double lostsSplitEcnashment,
			@Param("encTypeByLostsId") Integer encTypeByLostsId, @Param("encPriceCashIn") Double encPriceCashIn,
			@Param("encPriceCashOut") Double encPriceCashOut, @Param("encPriceBothInOut") Double encPriceBothInOut);

	@Delete("DELETE FROM T_CM_ENC_PLAN_DENOM WHERE ENC_PLAN_ID = #{encPlanId} ")
	void deleteEncPlanDenom(@Param("encPlanId") Integer encPlanId);

	@Delete("DELETE FROM T_CM_ENC_PLAN_CURR WHERE ENC_PLAN_ID = #{encPlanId} ")
	void deleteEncPlanCurr(@Param("encPlanId") Integer encPlanId);

	@Insert("Insert into T_CM_ENC_PLAN_CURR (ENC_PLAN_ID, CURR_CODE, CURR_SUMM, CURR_AVG_DEMAND) VALUES "
			+ " (#{encPlanId}, #{currCode}, #{countInOneCassPlan}, #{denom})")
	void insertEncPlanCurr(@Param("encPlanId") Integer encPlanId, @Param("currCode") Integer currCode,
			@Param("countInOneCassPlan") Long countInOneCassPlan, @Param("denom") Long denom);

	@Insert("Insert into T_CM_ENC_PLAN_DENOM (ENC_PLAN_ID, DENOM_CURR, DENOM_COUNT, DENOM_VALUE) VALUES "
			+ " (#{encPlanId}, #{currCode}, #{countInOneCassPlan}, #{denom})")
	void insertEncPlanDenom(@Param("encPlanId") Integer encPlanId, @Param("currCode") Integer currCode,
			@Param("countInOneCassPlan") Integer countInOneCassPlan, @Param("denom") Integer denom);

	@Insert("Insert into T_CM_TEMP_ENC_PLAN (ENC_PLAN_ID, ATM_ID, DATE_FORTHCOMING_ENCASHMENT) VALUES (#{encPlanId}, #{atmId}, #{encDate})")
	void insertForecastDataForParticularDate_toTempEncPlan(@Param("encPlanId") Integer encPlanId,
			@Param("atmId") Integer atmId, @Param("encDate") Timestamp encDate);

	@Insert("Insert into T_CM_TEMP_ENC_PLAN_CURR (ENC_PLAN_ID, CURR_CODE, CURR_SUMM) VALUES "
			+ " (#{encPlanId}, #{curr}, #{planSumm})")
	void insertForecastDataForParticularDate_toTempEncPlanCurr(@Param("encPlanId") Integer encPlanId,
			@Param("curr") Integer curr, @Param("planSumm") Long planSumm);

	@Insert("Insert into T_CM_TEMP_ENC_PLAN_DENOM (ENC_PLAN_ID, DENOM_CURR, DENOM_COUNT, DENOM_VALUE) "
			+ " VALUES (#{encPlanId}, #{curr}, #{countInOneCassPlan}, #{denom})")
	void insertForecastDataForParticularDate_toTempEncPlanDenom(@Param("encPlanId") Integer encPlanId,
			@Param("curr") Integer curr, @Param("countInOneCassPlan") Integer countInOneCassPlan,
			@Param("denom") Integer denom);

	@DeleteProvider(type = EncashmentsInsertBuilder.class, method = "deleteEncashmentsBuilder_encPlanDenom")
	void deleteEncashments_deleteTempTable(@Param("encList") List<Integer> encList, @Param("table") String table);
	
	@Result(column = "ENC_PLAN_ID", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT ENC_PLAN_ID as FROM T_CM_ENC_PLAN WHERE ATM_ID = #{atmId} AND DATE_FORTHCOMING_ENCASHMENT >= #{dateForCheck} "
			+ "AND IS_APPROVED = 0")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> getEncPlanId(@Param("atmId") Integer atmId, @Param("dateForCheck") Date dateForCheck);
	
	@Result(column = "ENCASHMENT_TYPE", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT ep.ENCASHMENT_TYPE FROM V_CM_ENC_FORTHCOMING ep WHERE ep.ATM_ID = #{atmId} "
			+ "AND ep.DATE_FORTHCOMING_ENCASHMENT >= #{dateForCheck} ")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> getEncashmentType(@Param("atmId") Integer atmId, @Param("dateForCheck") Date dateForCheck);

	@Result(column = "ENC_PLAN_ID", javaType = Integer.class)
	@ResultType(Integer.class)
	@SelectProvider(type = EncashmentsInsertBuilder.class, method = "getExistingPlanIdBuilder")
	Integer getExistingPlanId(@Param("atmId") Integer atmId, @Param("dateForCheck") Date dateForCheck,
			@Param("limit") String limit);
	
	@ConstructorArgs({
		@Arg(column = "ROUTE_STATUS", javaType = Integer.class),
		@Arg(column = "DATE_FORTHCOMING_ENCASHMENT", javaType = Date.class)
	})
	@ResultType(ObjectPair.class)
	@Select("Select route.ROUTE_STATUS, pnt.VISITED_FLAG, plan.DATE_FORTHCOMING_ENCASHMENT from T_CM_ROUTE route, T_CM_ROUTE_POINT pnt, T_CM_ENC_PLAN plan "
			+ "where route.ID=pnt.route_id and pnt.point_src_id=plan.enc_plan_id and plan.enc_plan_id=#{encPlanId}")
	ObjectPair<Integer, Date> ensureRouteConsistencyForEnc_route(@Param("encPlanId") Integer encPlanId);

	@Delete("delete from T_CM_ROUTE_POINT where POINT_SRC_ID=#{srcId}")
	void ensureRouteConsistencyForEnc_deleteRoutePoint(@Param("srcId") Integer srcId);
	
	@ConstructorArgs({
		@Arg(column = "ORG_ID", javaType = Integer.class),
		@Arg(column = "ROUTE_DATE", javaType = Date.class)
	})
	@ResultType(ObjectPair.class)
	@Select("Select route.ID, route.ORG_ID, route.ROUTE_DATE from T_CM_ROUTE route where route.ID=#{id}")
	ObjectPair<Integer, Date> getRouteById(@Param("id") Integer id);
	
	@ResultType(Integer.class)
	@Result(column = "ID", javaType = Integer.class)
	@Select("Select route.ID from T_CM_ROUTE route, T_CM_ROUTE_POINT pnt where route.ID=pnt.ROUTE_ID and pnt.POINT_SRC_ID=#{srcId}")
	Integer getRouteIdForEnc(@Param("srcId") Integer srcId);
	
	@ResultType(Integer.class)
	@Result(column = "ENCASHMENT_TYPE", javaType = Integer.class)
	@Select("SELECT ep.ENCASHMENT_TYPE FROM T_CM_ENC_PLAN ep WHERE ep.ATM_ID = #{atmId} "
			+ "AND ep.DATE_FORTHCOMING_ENCASHMENT >= #{dateForCheck} AND ep.IS_APPROVED = 1 AND "
			+ "( ep.ENCASHMENT_TYPE = #{encInType} AND NOT EXISTS (SELECT null FROM T_CM_ENC_CASHIN_STAT ecs "
			+ "WHERE ep.ATM_ID = ecs.ATM_ID AND ecs.CASH_IN_ENC_DATE >= ep.DATE_FORTHCOMING_ENCASHMENT) "
			+ "OR ep.ENCASHMENT_TYPE = #{encOutType} AND NOT EXISTS (SELECT null FROM T_CM_ENC_CASHOUT_STAT ecs "
			+ "WHERE ep.ATM_ID = ecs.ATM_ID AND ecs.ENC_DATE >= ep.DATE_FORTHCOMING_ENCASHMENT) "
			+ "OR ep.ENCASHMENT_TYPE = #{encBothType} AND NOT EXISTS (SELECT null FROM T_CM_ENC_CASHOUT_STAT ecs "
			+ "WHERE ep.ATM_ID = ecs.ATM_ID AND ecs.ENC_DATE >= ep.DATE_FORTHCOMING_ENCASHMENT) "
			+ "AND NOT EXISTS (SELECT null FROM T_CM_ENC_CASHIN_STAT ecs WHERE ep.ATM_ID = ecs.ATM_ID "
			+ "AND ecs.CASH_IN_ENC_DATE >= ep.DATE_FORTHCOMING_ENCASHMENT)) ")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> checkExistingEncashments_getEncType(@Param("atmId") Integer atmId,
			@Param("dateForCheck") Date dateForCheck, @Param("encInType") Integer encInType,
			@Param("encOutType") Integer encOutType, @Param("encBothType") Integer encBothType);
}