package ru.bpc.cm.encashments.orm;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;
import org.apache.ibatis.annotations.UpdateProvider;
import org.apache.ibatis.session.SqlSession;

import ru.bpc.cm.cashmanagement.orm.builders.AtmEncashmentBuilder;
import ru.bpc.cm.cashmanagement.orm.items.FullCodesDto;
import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.encashments.AtmEncashmentController;
import ru.bpc.cm.items.encashments.AtmCurrStatItem;
import ru.bpc.cm.items.encashments.AtmEncRequestEncItem;
import ru.bpc.cm.items.encashments.AtmEncRequestItem;
import ru.bpc.cm.items.encashments.AtmEncSubmitFilter;
import ru.bpc.cm.items.encashments.AtmEncSubmitItem;
import ru.bpc.cm.items.encashments.AtmEncashmentLogItem;
import ru.bpc.cm.items.encashments.AtmPeriodEncashmentItem;
import ru.bpc.cm.items.encashments.EncashmentCassItem;
import ru.bpc.cm.items.encashments.EncashmentDetailsItem;
import ru.bpc.cm.items.encashments.EncashmentItem;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.Pair;

/**
 * Интерфейс-маппер для класса {@link AtmEncashmentController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 12.03.2017
 * @version 1.0.0
 *
 */
public interface AtmEncashmentMapper extends IMapper {

	@ConstructorArgs({
		@Arg(column = "ENC_PLAN_ID", javaType = Integer.class),
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "DATE_FORTHCOMING_ENCASHMENT", javaType = Timestamp.class),
		@Arg(column = "EMERGENCY_ENCASHMENT", javaType = Boolean.class),
		@Arg(column = "CONFIRM_ID", javaType = Integer.class),
		@Arg(column = "STATE", javaType = String.class),
		@Arg(column = "CITY", javaType = String.class),
		@Arg(column = "STREET", javaType = String.class),
		@Arg(column = "APPROVE_NAME", javaType = String.class),
		@Arg(column = "CONFIRM_NAME", javaType = String.class),
		@Arg(column = "REQUEST_NAME", javaType = String.class),
		@Arg(column = "REQUEST_ID", javaType = Integer.class),
		@Arg(column = "APPROVE_LOGIN", javaType = String.class),
		@Arg(column = "CONFIRM_LOGIN", javaType = String.class),
		@Arg(column = "CASH_ADD_ENCASHMENT", javaType = Boolean.class),
		@Arg(column = "ATM_NAME", javaType = String.class),
		@Arg(column = "ENCASHMENT_TYPE", javaType = Integer.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class)		
	})
	@ResultType(AtmEncSubmitItem.class)
	@SelectProvider(type = AtmEncashmentBuilder.class, method = "getAtmEncashmentListBuilder")
	@Options(useCache = true, fetchSize = 1000)
	List<AtmEncSubmitItem> getAtmEncashmentList(@Param("filter") AtmEncSubmitFilter addFilter,
			@Param("dateFrom") Timestamp dateFrom, @Param("dateTo") Timestamp dateTo,
			@Param("personId") Integer personId);
	
	@ConstructorArgs({
		@Arg(column = "ENC_PLAN_ID", javaType = Integer.class),
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "DATE_FORTHCOMING_ENCASHMENT", javaType = Timestamp.class),
		@Arg(column = "EMERGENCY_ENCASHMENT", javaType = Boolean.class),
		@Arg(column = "STATE", javaType = String.class),
		@Arg(column = "CITY", javaType = String.class),
		@Arg(column = "STREET", javaType = String.class),
		@Arg(column = "APPROVE_NAME", javaType = String.class),
		@Arg(column = "CONFIRM_NAME", javaType = String.class),
		@Arg(column = "ENC_REQ_ID", javaType = Integer.class),
		@Arg(column = "APPROVE_LOGIN", javaType = String.class),
		@Arg(column = "CONFIRM_LOGIN", javaType = String.class),
		@Arg(column = "CASH_ADD_ENCASHMENT", javaType = Boolean.class),
		@Arg(column = "ATM_NAME", javaType = String.class),
		@Arg(column = "ENCASHMENT_TYPE", javaType = Integer.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class)		
	})
	@ResultType(AtmEncRequestEncItem.class)
	@Select("SELECT ENC_PLAN_ID, aep.ATM_ID, aep.DATE_PREVIOUS_ENCASHMENT, aep.DATE_FORTHCOMING_ENCASHMENT, "
			+ " IS_APPROVED, EMERGENCY_ENCASHMENT,CONFIRM_ID, u.NAME as APPROVE_NAME,u2.NAME as CONFIRM_NAME, "
			+ " u.LOGIN as APPROVE_LOGIN, u2.LOGIN as CONFIRM_LOGIN, "
			+ "ai.STATE,ai.CITY,ai.STREET,ai.NAME as ATM_NAME, ENC_REQ_ID, aep.CASH_ADD_ENCASHMENT,aep.ENCASHMENT_TYPE ,"
			+ "ai.EXTERNAL_ATM_ID FROM T_CM_ENC_PLAN aep join T_CM_ATM ai on (ai.ATM_ID = aep.ATM_ID) "
			+ "join T_CM_USER u on (u.ID = aep.APPROVE_ID)  "
			+ "left outer join T_CM_USER u2 on (u2.ID = aep.CONFIRM_ID) WHERE ENC_REQ_ID = #{encReqId} "
			+ "ORDER BY ATM_ID")
	@Options(useCache = true, fetchSize = 1000)
	List<AtmEncRequestEncItem> getEncReqEncashments(@Param("encReqId") Integer encReqId);
	
	@ConstructorArgs({
		@Arg(column = "ENC_PLAN_ID", javaType = Integer.class),
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "DATE_FORTHCOMING_ENCASHMENT", javaType = Timestamp.class),
		@Arg(column = "EMERGENCY_ENCASHMENT", javaType = Boolean.class),
		@Arg(column = "STATE", javaType = String.class),
		@Arg(column = "CITY", javaType = String.class),
		@Arg(column = "STREET", javaType = String.class),
		@Arg(column = "ENC_REQ_ID", javaType = Integer.class),
		@Arg(column = "APPROVE_LOGIN", javaType = String.class),
		@Arg(column = "CONFIRM_LOGIN", javaType = String.class),
		@Arg(column = "ENCASHMENT_TYPE", javaType = Integer.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class)		
	})
	@Select("SELECT ENC_PLAN_ID, aep.ATM_ID, ai.EXTERNAL_ATM_ID, DATE_FORTHCOMING_ENCASHMENT, "
			+ "ENCASHMENT_TYPE, EMERGENCY_ENCASHMENT, aep.ENC_REQ_ID, ai.STATE,ai.CITY,ai.STREET, "
			+ "u.LOGIN as APPROVE_LOGIN, ENC_REQ_ID, u2.LOGIN as CONFIRM_LOGIN FROM T_CM_ENC_PLAN aep "
			+ "join T_CM_ATM ai on (ai.ATM_ID = aep.ATM_ID) "
			+ "left outer join T_CM_USER u on (u.ID = aep.APPROVE_ID)  "
			+ "left outer join T_CM_USER u2 on (u2.ID = aep.CONFIRM_ID) WHERE ENC_PLAN_ID = #{encPlanId}")
	@ResultType(EncashmentItem.class)
	@Options(useCache = true)
	EncashmentItem getEncashmentById(@Param("encPlanId") Integer encPlanId);
	
	@ConstructorArgs({
		@Arg(column = "ENC_PLAN_ID", javaType = Integer.class),
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "DATE_FORTHCOMING_ENCASHMENT", javaType = Timestamp.class),
		@Arg(column = "EMERGENCY_ENCASHMENT", javaType = Boolean.class),
		@Arg(column = "STATE", javaType = String.class),
		@Arg(column = "CITY", javaType = String.class),
		@Arg(column = "STREET", javaType = String.class),
		@Arg(column = "ENC_REQ_ID", javaType = Integer.class),
		@Arg(column = "APPROVE_LOGIN", javaType = String.class),
		@Arg(column = "CONFIRM_LOGIN", javaType = String.class),
		@Arg(column = "ENCASHMENT_TYPE", javaType = Integer.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class)		
	})
	@Select("SELECT ENC_PLAN_ID, aep.ATM_ID, ai.EXTERNAL_ATM_ID, DATE_FORTHCOMING_ENCASHMENT, "
			+ "ENCASHMENT_TYPE, EMERGENCY_ENCASHMENT, aep.ENC_REQ_ID, ai.STATE,ai.CITY,ai.STREET, "
			+ "u.LOGIN as APPROVE_LOGIN, u2.LOGIN as CONFIRM_LOGIN FROM T_CM_ENC_PLAN aep "
			+ "join T_CM_ATM ai on (ai.ATM_ID = aep.ATM_ID) "
			+ "left outer join T_CM_USER u on (u.ID = aep.APPROVE_ID)  "
			+ "left outer join T_CM_USER u2 on (u2.ID = aep.CONFIRM_ID) WHERE ENC_PLAN_ID = #{encPlanId}")
	@ResultType(EncashmentItem.class)
	@Options(useCache = true)
	EncashmentItem getEncashmentById_sec(@Param("encPlanId") Integer encPlanId);
	
	@Result(column = "DENOM_CURR", javaType = Integer.class)
	@ResultType(Integer.class)
	@SelectProvider(type = AtmEncashmentBuilder.class, method = "getCurrDenomBuilder")
	@Options(useCache = true)
	List<Integer> getCurrDenom(@Param("encPlanIDList") List<Integer> encPlanIDList);
	
	@Results({
		@Result(column = "DENOM_COUNT", property = "denomCount", javaType = Integer.class),
		@Result(column = "DENOM_VALUE", property = "denomValue", javaType = Integer.class),
		@Result(column = "DENOM_CURR", property = "denomCurr", javaType = Integer.class),
		@Result(column = "CODE_A3", property = "denomCurrA3", javaType = String.class)
	})
	@ResultType(EncashmentCassItem.class)
	@Select("SELECT DENOM_VALUE, DENOM_COUNT, DENOM_CURR , CODE_A3 FROM T_CM_ENC_PLAN_DENOM "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE ENC_PLAN_ID = #{encPlanId} "
			+ "ORDER BY DENOM_CURR,DENOM_VALUE DESC,DENOM_COUNT ")
	@Options(useCache = true)
	List<EncashmentCassItem> getAtmEncashmentCassList(@Param("encPlanId") Integer encPlanId);

	@Results({
		@Result(column = "MAIN_CURR_CODE", property = "mainCurrCode", javaType = String.class),
		@Result(column = "MAIN_CURR_SUMM", property = "mainCurrSumm", javaType = String.class),
		@Result(column = "MAIN_CURR_AVG_DEMAND", property = "mainCurAvgDemand", javaType = String.class),
		@Result(column = "MAIN_CURR_CODE_A3", property = "mainCurrCodeA3", javaType = String.class),
		@Result(column = "SECONDARY_CURR_CODE", property = "secCurrCode", javaType = String.class),
		@Result(column = "SEC_CURR_SUMM", property = "secCurrSumm", javaType = String.class),
		@Result(column = "SEC_CURR_AVG_DEMAND", property = "secCurAvgDemand", javaType = String.class),
		@Result(column = "SEC_CURR_CODE_A3", property = "secCurrCodeA3", javaType = String.class),
		@Result(column = "SECONDARY2_CURR_CODE", property = "sec2CurrCode", javaType = String.class),
		@Result(column = "SEC2_CURR_SUMM", property = "sec2CurrSumm", javaType = String.class),
		@Result(column = "SEC2_CURR_AVG_DEMAND", property = "sec2CurAvgDemand", javaType = String.class),
		@Result(column = "SEC2_CURR_CODE_A3", property = "sec2CurrCodeA3", javaType = String.class),
		@Result(column = "SECONDARY3_CURR_CODE", property = "sec3CurrCode", javaType = String.class),
		@Result(column = "SEC3_CURR_SUMM", property = "sec3CurrSumm", javaType = String.class),
		@Result(column = "SEC3_CURR_AVG_DEMAND", property = "sec3CurAvgDemand", javaType = String.class),
		@Result(column = "SEC3_CURR_CODE_A3", property = "sec3CurrCodeA3", javaType = String.class)
	})
	@ResultType(FullCodesDto.class)
	@Select("SELECT MAIN_CURR_CODE, MAIN_CURR_SUMM, MAIN_CURR_AVG_DEMAND, MAIN_CURR_CODE_A3, "
			+ "SECONDARY_CURR_CODE, SEC_CURR_SUMM, SEC_CURR_AVG_DEMAND, SEC_CURR_CODE_A3, "
			+ "SECONDARY2_CURR_CODE, SEC2_CURR_SUMM, SEC2_CURR_AVG_DEMAND, SEC2_CURR_CODE_A3, "
			+ "SECONDARY3_CURR_CODE, SEC3_CURR_SUMM, SEC3_CURR_AVG_DEMAND, SEC3_CURR_CODE_A3 "
			+ "FROM V_CM_ENC_PLAN_CURR WHERE ENC_PLAN_ID = #{encPlanId}")
	@Options(useCache = true)
	FullCodesDto getAtmEncashmentCurrencies(@Param("encPlanId") Integer encPlanId);
	
	@Results({
		@Result(column = "ID", property = "key", javaType = Integer.class),
		@Result(column = "DESCRIPTION", property = "value", javaType = String.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT ag.ID,ag.DESCRIPTION FROM T_CM_ATM_GROUP ag "
			+ "join T_CM_ATM2ATM_GROUP agr on(agr.atm_group_id = ag.id) WHERE 1=1 AND agr.ATM_ID = #{atmId} "
			+ "AND ag.type_id = #{typeId}")
	@Options(useCache = true)
	ObjectPair<Integer, String> getAtmEncashmentMaxAtmCount(@Param("atmId") Integer atmId,
			@Param("typeId") Integer typeId);

	@Result(column = "ENC_COUNT", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT count(distinct ATM_ID) as ENC_COUNT FROM T_CM_ENC_PLAN WHERE 1=1 "
			+ "AND DATE_FORTHCOMING_ENCASHMENT = #{date} AND ATM_ID IN "
			+ "(SELECT ATM_ID FROM T_CM_ATM2ATM_GROUP WHERE ATM_GROUP_ID = #{maxCount})")
	@Options(useCache = true, fetchSize = 1000)
	Integer getAtmEncashmentMaxAtmCount_count(@Param("date") Date date,
			@Param("maxCount") Integer maxCount);

	@UpdateProvider(type = AtmEncashmentBuilder.class, method = "approveSelectedEncahmentsBuilder")
	void approveSelectedEncahments(@Param("selected") List<Integer> selected, @Param("personId") Integer personId);

	@UpdateProvider(type = AtmEncashmentBuilder.class, method = "confirmSelectedEncahmentsBuilder")
	void confirmSelectedEncahments(@Param("selected") List<Integer> selected, @Param("personId") Integer personId);

	@Update("UPDATE T_CM_ENC_PLAN SET IS_APPROVED = 0, CONFIRM_ID = 0 WHERE ENC_PLAN_ID = #{encPlanId} ")
	void discardEncashment(@Param("encPlanId") Integer encPlanId);

	@UpdateProvider(type = AtmEncashmentBuilder.class, method = "discardEncashmentsDateChangeBuilder")
	void discardEncashmentsDateChange(@Param("selected") List<Integer> selected);
	
	@ConstructorArgs({
		@Arg(column = "LOG_DATE", javaType = Timestamp.class),
		@Arg(column = "NAME", javaType = String.class),
		@Arg(column = "MESSAGE", javaType = String.class),
		@Arg(column = "MESSAGE_TYPE", javaType = Integer.class),
		@Arg(column = "ID", javaType = Integer.class),
		@Arg(column = "LOG_ID", javaType = Integer.class),
		@Arg(column = "MESSAGE_PARAMS", javaType = String.class)
	})
	@Select("SELECT el.LOG_DATE,u.ID, COALESCE(u.NAME,'SYSTEM') as NAME, "
			+ "el.MESSAGE, el.MESSAGE_TYPE, el.LOG_ID,el.MESSAGE_PARAMS FROM T_CM_ENC_PLAN_LOG el "
			+ "left outer join T_cm_USER u on (u.id = el.USER_ID) WHERE ENC_PLAN_ID = #{encPlanId} "
			+ "ORDER BY LOG_DATE DESC")
	@ResultType(AtmEncashmentLogItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<AtmEncashmentLogItem> getAtmEncashmentLogList(@Param("encPlanId") Integer encPlanId);

	@UpdateProvider(type = AtmEncashmentBuilder.class, method = "updateReqIdForSelectedEncashmnetsBuilder")
	void updateReqIdForSelectedEncashmnets(@Param("selected") List<Integer> selected,
			@Param("encReqId") Integer encReqId);

	@UpdateProvider(type = AtmEncashmentBuilder.class, method = "updateReqIdForSelectedEncashmnetsBuilder")
	void updateReqDateForSelectedEncashmnetsEncashmnets(@Param("selected") List<Integer> selected,
			@Param("reqDate") Timestamp reqDate);
	
	@Results({
		@Result(column = "DESCRIPTION", property = "description", javaType = String.class),
		@Result(column = "ID", property = "id", javaType = Integer.class),
		@Result(column = "NAME", property = "name", javaType = String.class),
		@Result(column = "REQUEST_DATE", property = "requestDate", javaType = Timestamp.class),
		@Result(column = "USER_ID", property = "userId", javaType = Integer.class)
	})
	@Select("SELECT er.ID, er.REQUEST_DATE, er.NAME as NAME, er.DESCRIPTION,er.USER_ID "
			+ "FROM T_CM_ENC_PLAN_REQUEST er WHERE (er.USER_ID = #{personId} OR #{isFetchAll} = 1) AND er.REQUEST_DATE >= #{startDate} "
			+ "AND er.REQUEST_DATE <= #{endDate} AND (er.ID = #{reqId} OR #{reqId} = 0) ORDER BY REQUEST_DATE")
	@Options(useCache = true, fetchSize = 1000)
	List<AtmEncRequestItem> getEncashmentRequests_withStartDate(@Param("personId") Integer personId,
			@Param("isFetchAll") Integer isFetchAll, @Param("startDate") Timestamp startDate,
			@Param("endDate") Timestamp endDate, @Param("reqId") Integer reqId);
	
	@Results({
		@Result(column = "DESCRIPTION", property = "description", javaType = String.class),
		@Result(column = "ID", property = "id", javaType = Integer.class),
		@Result(column = "NAME", property = "name", javaType = String.class),
		@Result(column = "REQUEST_DATE", property = "requestDate", javaType = Timestamp.class),
		@Result(column = "USER_ID", property = "userId", javaType = Integer.class)
	})
	@Select("SELECT er.ID, er.REQUEST_DATE, er.NAME as NAME, er.DESCRIPTION,er.USER_ID "
			+ "FROM T_CM_ENC_PLAN_REQUEST er WHERE er.USER_ID = #{personId} AND "
			+ "er.REQUEST_DATE >= #{date} ORDER BY REQUEST_DATE")
	@Options(useCache = true, fetchSize = 1000)
	List<AtmEncRequestItem> getEncashmentRequests(@Param("personId") Integer personId, @Param("date") Date date);

	@Result(column = "REQ_ID", javaType = Integer.class)
	@ResultType(Integer.class)
	@SelectProvider(type = AtmEncashmentBuilder.class, method = "generateEncashmentRequestId")
	Integer generateEncashmentRequestId(@Param("session") SqlSession session);
	
	@Insert("INSERT INTO T_CM_ENC_PLAN_REQUEST (ID,REQUEST_DATE,NAME,DESCRIPTION,USER_ID) VALUES "
			+ "(#{id},#{date},#{name},#{dsc},#{userId})")
	void changeEncashmnetRequest_insert(@Param("id") Integer id, @Param("date") Date date, @Param("name") String name,
			@Param("dsc") String dsc, @Param("userId") Integer userId);

	@Update("UPDATE T_CM_ENC_PLAN SET ENC_REQ_ID = 0 WHERE ENC_REQ_ID = #{id}")
	void changeEncashmnetRequest_update2delete(@Param("id") Integer id);

	@Delete("DELETE FROM T_CM_ENC_PLAN_REQUEST WHERE ID = #{id}")
	void changeEncashmnetRequest_delete(@Param("id") Integer id);

	@Update("UPDATE T_CM_ENC_PLAN_REQUEST SET NAME = #{name}, DESCRIPTION = #{dsc} WHERE ID = #{id}")
	void changeEncashmnetRequest_update(@Param("name") String name, @Param("dsc") String dsc, @Param("id") Integer id);
	
	@ConstructorArgs({
		@Arg(column = "EMERGENCY_ENCASHMENT", javaType = Boolean.class),
		@Arg(column = "DATE_FORTHCOMING_ENCASHMENT", javaType = Timestamp.class),
		@Arg(column = "FORECAST_RESP_CODE", javaType = Integer.class),
		@Arg(column = "ID", javaType = Integer.class),
		@Arg(column = "ENCASHMENT_TYPE", javaType = Integer.class)
	})
	@Select("SELECT ID,DATE_FORTHCOMING_ENCASHMENT,ENCASHMENT_TYPE, FORECAST_RESP_CODE,"
			+ "CASH_IN_EXISTS,EMERGENCY_ENCASHMENT FROM T_CM_ENC_PERIOD WHERE ATM_ID = #{atmId} "
			+ "AND DATE_FORTHCOMING_ENCASHMENT <= #{endDate} ")
	@Options(useCache = true, fetchSize = 1000)
	@ResultType(AtmPeriodEncashmentItem.class)
	List<AtmPeriodEncashmentItem> getEncashmentsForPeriod(@Param("atmId") Integer atmId,
			@Param("endDate") Timestamp endDate);
	
	@ConstructorArgs({
		@Arg(column = "CURR_SUMM", javaType = String.class),
		@Arg(column = "CURR_CODE_A3", javaType = String.class)
	})
	@Select("SELECT CURR_CODE_A3,CURR_SUMM FROM T_CM_ENC_PERIOD_CURR WHERE ENC_PERIOD_ID = #{encPeriodId} ")
	@Options(useCache = true, fetchSize = 1000)
	@ResultType(Pair.class)
	List<Pair> getEncashmentCurrsForPeriod(@Param("encPeriodId") Integer encPeriodId);
	
	@Results({
		@Result(column = "DENOM_COUNT", property = "denomCount", javaType = Integer.class),
		@Result(column = "DENOM_VALUE", property = "denomValue", javaType = Integer.class),
		@Result(column = "DENOM_CURR", property = "denomCurr", javaType = Integer.class),
		@Result(column = "CODE_A3", property = "denomCurrA3", javaType = String.class)
	})
	@Select("SELECT DENOM_VALUE, DENOM_COUNT, DENOM_CURR , CODE_A3 FROM T_CM_ENC_PERIOD_DENOM "
			+ "join T_CM_CURR ci on (DENOM_CURR = ci.code_n3) WHERE ENC_PERIOD_ID = #{encPeriodId} "
			+ "ORDER BY DENOM_CURR,DENOM_VALUE DESC")
	@Options(useCache = true, fetchSize = 1000)
	@ResultType(EncashmentCassItem.class)
	List<EncashmentCassItem> getEncashmentDenomsForPeriod(@Param("encPeriodId") Integer encPeriodId);
	
	@Results({
		@Result(column = "CURR_CODE", property = "currCodeN3", javaType = Integer.class),
		@Result(column = "EMERGENCY_ENCASHMENT", property = "emergencyEncashment", javaType = Boolean.class),
		@Result(column = "CO_CURR_SUMM", property = "coSummTakeOff", javaType = Long.class),
		@Result(column = "CO_REMAINING_END_DAY", property = "coRemainingEndDay", javaType = Long.class),
		@Result(column = "CO_REMAINING_START_DAY", property = "coRemainingStartDay", javaType = Long.class),
		@Result(column = "CI_CURR_SUMM", property = "ciSummInsert", javaType = Long.class),
		@Result(column = "CI_REMAINING_END_DAY", property = "ciRemainingEndDay", javaType = Long.class),
		@Result(column = "CI_REMAINING_START_DAY", property = "ciRemainingStartDay", javaType = Long.class),
		@Result(column = "CR_CURR_SUMM_OUT", property = "crSummTakeOff", javaType = Long.class),
		@Result(column = "CR_CURR_SUMM_IN", property = "crSummInsert", javaType = Long.class),
		@Result(column = "CR_REMAINING_END_DAY", property = "crRemainingEndDay", javaType = Long.class),
		@Result(column = "CR_REMAINING_START_DAY", property = "crRemainingStartDay", javaType = Long.class),
		@Result(column = "STAT_DATE", property = "statDate", javaType = Timestamp.class),
		@Result(column = "CURR_LOADED_FROM_ATM", property = "summEncFromAtm", javaType = Long.class),
		@Result(column = "CURR_LOADED_TO_ATM", property = "summEncToAtm", javaType = Long.class),
		@Result(column = "FORECAST", property = "forecast", javaType = Boolean.class),
		@Result(column = "CASH_ADD_ENCASHMENT", property = "cashAddEncashment", javaType = Boolean.class)
	})
	@Select("SELECT ATM_ID,STAT_DATE,CURR_CODE, CO_CURR_SUMM, CO_REMAINING_START_DAY, CO_REMAINING_END_DAY,"
			+ "CI_CURR_SUMM, CI_REMAINING_START_DAY, CI_REMAINING_END_DAY,"
			+ "CR_CURR_SUMM_IN, CR_CURR_SUMM_OUT, CR_REMAINING_START_DAY, CR_REMAINING_END_DAY,"
			+ "CURR_LOADED_TO_ATM,CURR_LOADED_FROM_ATM,EMERGENCY_ENCASHMENT,FORECAST,CASH_ADD_ENCASHMENT "
			+ "FROM T_CM_ENC_PERIOD_STAT WHERE ATM_ID = #{atmId} AND CURR_CODE = #{curr} AND STAT_DATE <= #{endDate} "
			+ "ORDER BY STAT_DATE ")
	@Options(useCache = true, fetchSize = 1000)
	@ResultType(AtmCurrStatItem.class)
	List<AtmCurrStatItem> getCurrenciesForPeriod(@Param("atmId") Integer atmId, @Param("curr") Integer curr,
			@Param("endDate") Timestamp endDate);
	
	@ConstructorArgs({
		@Arg(column = "ATM_ID", javaType = Integer.class),
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class),
		@Arg(column = "ENC_PRICE_CASH_IN", javaType = Long.class),
		@Arg(column = "ENC_PRICE_CASH_OUT", javaType = Long.class),
		@Arg(column = "ENCASHMENT_TYPE", javaType = Integer.class),
		@Arg(column = "ENCASHMENT_TYPE_BY_LOSTS", javaType = Integer.class),
		@Arg(column = "ENC_LOSTS_CURR_CODE_A3", javaType = String.class),
		@Arg(column = "ENC_LOSTS_JOINT", javaType = Long.class),
		@Arg(column = "ENC_LOSTS_SPLIT", javaType = Long.class),
		@Arg(column = "ATM_TYPE", javaType = Integer.class)
	})
	@Select("SELECT aep.ATM_ID, ai.EXTERNAL_ATM_ID, ai.TYPE as ATM_TYPE, ENCASHMENT_TYPE_BY_LOSTS, ENC_LOSTS_JOINT, ENC_LOSTS_SPLIT,  "
			+ "ENCASHMENT_TYPE, ci.CODE_A3 as ENC_LOSTS_CURR_CODE_A3, "
			+ "ENC_PRICE_CASH_IN, ENC_PRICE_CASH_OUT, ENC_PRICE_BOTH_IN_OUT FROM T_CM_ENC_PLAN aep "
			+ "left outer join T_CM_CURR ci on(aep.ENC_LOSTS_CURR_CODE = ci.code_n3) "
			+ "join T_CM_ATM ai on (ai.ATM_ID = aep.ATM_ID) WHERE ENC_PLAN_ID = #{encPlanId}")
	@Options(useCache = true, fetchSize = 1000)
	@ResultType(EncashmentDetailsItem.class)
	EncashmentDetailsItem getEncashmentDetails(@Param("encPlanId") Integer encPlanId);

	@Insert("INSERT INTO t_cm_temp_enc_report (REMAINING, CURR_CODE, STAT_DATE, END_OF_STATS_DATE)"
			+ "VALUES(#{remaining},#{currCodeA3},#{statDate},#{isEndOfStatsDate})")
	void insertTempEncReport(@Param("remaining") Long remaining, @Param("currCodeA3") String currCodeA3,
			@Param("statDate") Timestamp statDate, @Param("isEndOfStatsDate") Boolean isEndOfStatsDate);
}
