package ru.bpc.cm.cashmanagement.orm;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
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

import ru.bpc.cm.cashmanagement.CmCommonController;
import ru.bpc.cm.cashmanagement.orm.builders.CmCommonBuilder;
import ru.bpc.cm.cashmanagement.orm.items.CodesItem;
import ru.bpc.cm.cashmanagement.orm.items.ConvertionsRateItem;
import ru.bpc.cm.cashmanagement.orm.items.FullAddressItem;
import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.Pair;

/**
 * Интерфейс-маппер для класса {@link CmCommonController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 24.02.2017
 * @version 1.0.1
 *
 */
public interface CmCommonMapper extends IMapper {

	@Result(column = "ENCASHMENT_ID", javaType = Integer.class)
	@Select("SELECT ENCASHMENT_ID FROM T_CM_ENC_CASHOUT_STAT WHERE ENC_DATE = #{encDate} and ATM_ID = #{atmId}")
	Integer getEncID(@Param("encDate") Timestamp encDate, @Param("atmId") Integer atmId);

	@Result(column = "EMERGENCY_ENCASHMENT", javaType = Boolean.class)
	@Select("SELECT EMERGENCY_ENCASHMENT FROM T_CM_ENC_CASHOUT_STAT WHERE ENC_DATE = #{encDate} and ATM_ID = #{atmId}")
	Boolean getEncEmergency(@Param("encDate") Timestamp encDate, @Param("atmId") Integer atmId);

	@ConstructorArgs({
		@Arg(column = "MAIN_CURR_CODE", javaType = Integer.class),
		@Arg(column = "SECONDARY_CURR_CODE", javaType = Integer.class),
		@Arg(column = "SECONDARY2_CURR_CODE", javaType = Integer.class),
		@Arg(column = "SECONDARY3_CURR_CODE", javaType = Integer.class)
	})
	@Select("SELECT MAIN_CURR_CODE, SECONDARY_CURR_CODE, SECONDARY2_CURR_CODE, "
			+ "SECONDARY3_CURR_CODE FROM V_CM_ATM_CURR WHERE ATM_ID = #{atmId}")
	@Options(useCache = true, fetchSize = 1000)
	List<CodesItem> getAtmCurrencies(@Param("atmId") Integer atmId);
	
	@Result(column = "CURR_CODE")
	@ResultType(Integer.class)
	@Select("SELECT DISTINCT CURR_CODE FROM ( SELECT MAIN_CURR_CODE as CURR_CODE FROM V_CM_ATM_CURR "
			+ "UNION SELECT SECONDARY_CURR_CODE FROM V_CM_ATM_CURR UNION "
			+ "SELECT SECONDARY2_CURR_CODE FROM V_CM_ATM_CURR UNION SELECT SECONDARY3_CURR_CODE "
			+ "FROM V_CM_ATM_CURR ) WHERE CURR_CODE > 0")
	List<Integer> getAtmCurrencies();
	
	@Result(column = "DENOM")
	@ResultType(Integer.class)
	@Select("SELECT DENOM FROM T_CM_CURR_DENOM WHERE CURR_CODE = #{curr} ")
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> getCurrencyDenominations(@Param("curr") Integer curr);
	
	@Result(column = "CASH_IN_ENCASHMENT_ID")
	@ResultType(Integer.class)
	@Select("SELECT CASH_IN_ENCASHMENT_ID FROM T_CM_ENC_CASHIN_STAT WHERE CASH_IN_ENC_DATE = #{encDate} and ATM_ID = #{atmId}")
	Integer getCashInEncID(@Param("encDate") Timestamp encDate, @Param("atmId") Integer atmId);

	@Result(column = "MAIN_CURR_CODE")
	@ResultType(Integer.class)
	@Select("SELECT COALESCE(MAIN_CURR_CODE,0) as MAIN_CURR_CODE FROM T_CM_ATM WHERE ATM_ID = #{atmId}")
	Integer getMainCurrCode(@Param("atmId") Integer atmId);
	
	@Result(column = "MAIN_CURR_ENABLED")
	@ResultType(Boolean.class)
	@Select("SELECT COALESCE(MAIN_CURR_ENABLED,0) as MAIN_CURR_ENABLED FROM T_CM_ATM WHERE ATM_ID = #{atmId}")
	Boolean getMainCurrEnabled(@Param("atmId") Integer atmId);
	
	@Result(column = "CODE_A3")
	@ResultType(String.class)
	@Select("SELECT ci.code_a3 FROM T_CM_CURR ci WHERE ci.code_n3 = #{currCode}")
	String getCurrCodeA3(@Param("currCode") Integer currCode);

	@Result(column = "INST_ID")
	@ResultType(String.class)
	@Select("SELECT INST_ID FROM T_CM_ATM WHERE ATM_ID = #{atmId}")
	String getInstIdForAtm(@Param("atmId") Integer atmId);
	
	@Result(column = "CL_DAY_TYPE")
	@ResultType(Integer.class)
	@Select("SELECT DISTINCT CL_DAY_TYPE FROM T_CM_CALENDAR_DAYS WHERE CL_ID = (SELECT CALENDAR_ID FROM T_CM_ATM WHERE ATM_ID = #{atmId}")
	Integer getAtmTypeByDemand(@Param("atmId") Integer atmId);
	
	@Result(column = "TYPE")
	@ResultType(Integer.class)
	@Select("SELECT TYPE FROM T_CM_ATM WHERE ATM_ID = #{atmId}")
	Integer getAtmTypeByOperations(@Param("atmId") Integer atmId);

	@Result(column = "CNT")
	@ResultType(Integer.class)
	@Select("SELECT COUNT(1) as CNT FROM T_CM_ATM_CASSETTES WHERE ATM_ID = #{atmId} " + "AND CASS_TYPE = #{cassType} ")
	Integer getAtmCassCount(@Param("atmId") Integer atmId, @Param("cassType") Integer cassType);
	
	@Result(column = "SECONDARY_CURR_CODE")
	@ResultType(Integer.class)
	@Select("SELECT COALESCE(SECONDARY_CURR_CODE,0) as SECONDARY_CURR_CODE FROM T_CM_ATM WHERE ATM_ID = #{atmId} ")
	Integer getSecCurrCode(@Param("atmId") Integer atmId);
	
	@Result(column = "SECONDARY2_CURR_CODE")
	@ResultType(Integer.class)
	@Select("SELECT COALESCE(SECONDARY2_CURR_CODE,0) as SECONDARY2_CURR_CODE FROM T_CM_ATM WHERE ATM_ID = #{atmId} ")
	Integer getSec2CurrCode(@Param("atmId") Integer atmId);
	
	@Result(column = "SECONDARY3_CURR_CODE")
	@ResultType(Integer.class)
	@Select("SELECT COALESCE(SECONDARY3_CURR_CODE,0) as SECONDARY3_CURR_CODE FROM T_CM_ATM WHERE ATM_ID = #{atmId} ")
	Integer getSec3CurrCode(@Param("atmId") Integer atmId);
	
	@Result(column = "VALUE")
	@ResultType(String.class)
	@Select("select aga.value from T_CM_ATM2ATM_GROUP agr "
			+ "join T_CM_ATM_GROUP ag on (ag.id = agr.atm_group_id) left outer join T_CM_ATM_GROUP_ATTR aga on "
			+ "(agr.atm_group_id = aga.atm_group_id) where 1=1 AND aga.attr_id = #{attrId} AND ag.type_id = #{typeId} "
			+ "AND agr.ATM_ID = #{atmId} ")
	String getAtmAttribute(@Param("attrId") Integer attrId, @Param("typeId") Integer typeId,
			@Param("atmId") Integer atmId);
	
	@Results({
		@Result(column = "ATTR_ID", property = "key", javaType = Integer.class),
		@Result(column = "VALUE", property = "value", javaType = String.class)
	})
	@Select("select aga.attr_id ,aga.value from T_CM_ATM2ATM_GROUP agr "
			+ "join T_CM_ATM_GROUP ag on (ag.id = agr.atm_group_id) left outer join T_CM_ATM_GROUP_ATTR aga on "
			+ "(agr.atm_group_id = aga.atm_group_id) where 1=1 AND ag.type_id != #{typeId} "
			+ "AND agr.ATM_ID = #{atmId} ")
	@Options(useCache = true, fetchSize = 1000)
	List<ObjectPair<Integer, String>> getAtmAttributes(@Param("typeId") Integer typeId, @Param("atmId") Integer atmId);
	
	@Results({ 
		@Result(column = "MULTIPLE_FLAG", property = "key", javaType = String.class),
		@Result(column = "CNVT_RATE", property = "value", javaType = Double.class) 
	})
	@Select("SELECT CNVT_RATE, MULTIPLE_FLAG FROM T_CM_CURR_CONVERT_RATE WHERE DEST_CURR_CODE = #{destCurr} "
			+ "AND SRC_CURR_CODE = #{srcCurr} AND DEST_INST_ID = #{instId} AND SRC_INST_ID = #{instId} "
			+ "ORDER BY CNVT_DATE DESC")
	List<ObjectPair<String, Double>> convertValue(@Param("destCurr") Integer destCurr, @Param("srcCurr") Integer srcCurr,
			@Param("instId") String instId);
	
	@ConstructorArgs({ 
		@Arg(column = "src_curr_code", javaType = String.class),
		@Arg(column = "dest_curr_code", javaType = String.class),
		@Arg(column = "multiple_flag", javaType = String.class),
		@Arg(column = "cnvt_rate", javaType = Double.class)
	})
	@SelectProvider(type = CmCommonBuilder.class, method = "getConvertionsRatesBuilder")
	@Options(useCache = true, fetchSize = 1000)
	List<ConvertionsRateItem> getConvertionsRates(@Param("atmId") Integer atmId,
			@Param("currencies") List<Integer> currencies);
	
	@InsertProvider(type = CmCommonBuilder.class, method = "insertEncashmentMessage")
	void insertEncashmentMessage(@Param("nextSeq") String nextSeq, @Param("encPlanId") Integer encPlanId,
			@Param("currTime") Timestamp currTime, @Param("personId") Integer personId,
			@Param("message") String message, @Param("logType") Integer logType,
			@Param("printedCollection") String coll);
	
	@Delete("DELETE FROM T_CM_ENC_PLAN_LOG WHERE LOG_ID = #{logId} ")
	void deleteEncashmentMessage(@Param("logId") Integer logId);
	
	@Results({
		@Result(column = "CASH_OUT_ENCASHMENT_ID", property = "key", javaType = Integer.class),
		@Result(column = "CASH_OUT_STAT_DATE", property = "value", javaType = Date.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT CASH_OUT_ENCASHMENT_ID,CASH_OUT_STAT_DATE FROM T_CM_ATM_ACTUAL_STATE WHERE "
			+ " ATM_ID = #{atmId} ")
	ObjectPair<Integer, Date> getCashOutStatDate(@Param("atmId") Integer atmId);
	
	@Result(column = "state")
	@Select("select cass_state as state from T_CM_CASHOUT_CASS_STAT cs where cs.atm_id = #{atmId} "
			+ "AND cs.encashment_id = #{encId} AND cs.stat_date = #{statDate} ")
	@ResultType(Integer.class)
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> getCashOutStatus(@Param("atmId") Integer atmId, @Param("encId") Integer encId,
			@Param("statDate") Date statDate);
	
	@Results({
		@Result(column = "CASH_IN_ENCASHMENT_ID", property = "key", javaType = Integer.class),
		@Result(column = "CASH_IN_STAT_DATE", property = "value", javaType = Date.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT CASH_IN_ENCASHMENT_ID,CASH_IN_STAT_DATE FROM T_CM_ATM_ACTUAL_STATE WHERE ATM_ID = #{atmId} ")
	ObjectPair<Integer, Date> getCashInStatDate(@Param("atmId") Integer atmId);

	@Result(column = "state")
	@Select("select cash_in_state as state from T_CM_CASHIN_STAT cs where cs.atm_id = #{atmId} "
			+ "AND cs.cash_in_encashment_id = #{encId} AND cs.stat_date = #{statDate} ")
	@ResultType(Integer.class)
	@Options(useCache = true, fetchSize = 1000)
	List<Integer> getCashInStatus(@Param("atmId") Integer atmId, @Param("encId") Integer encId,
			@Param("statDate") Date statDate);
	
	@ConstructorArgs({
		@Arg(column = "STATE", javaType = String.class),
		@Arg(column = "CITY", javaType = String.class),
		@Arg(column = "STREET", javaType = String.class),
		@Arg(column = "NAME", javaType = String.class)
	})
	@ResultType(FullAddressItem.class)
	@Select("SELECT i.street, i.city, i.state, i.name FROM T_CM_ATM i WHERE 1=1 AND i.atm_ID = #{atmId}")
	FullAddressItem getAtmAddressAndName(@Param("atmId") Integer atmId);
	
	@ConstructorArgs({
		@Arg(column = "EXTERNAL_ATM_ID", javaType = String.class),
		@Arg(column = "NAME", javaType = String.class)
	})
	@ResultType(Pair.class)
	@Select("SELECT i.external_atm_id, i.name FROM T_CM_ATM i WHERE 1=1 AND i.atm_ID = #{atmId}")
	Pair getAtmExtIdAndName(@Param("atmId") Integer atmId);
	
	@ConstructorArgs({
		@Arg(column = "NAME", javaType = String.class),
		@Arg(column = "DESCRIPTION", javaType = String.class)
	})
	@ResultType(Pair.class)
	@Select("SELECT i.name,i.description FROM T_CM_ATM_GROUP i WHERE 1=1 AND i.ID = #{atmId}")
	Pair getAtmGroupNameAndDescx(@Param("atmId") Integer atmId);

	@Insert("INSERT INTO T_CM_TEMP_ATM_LIST VALUES(#{value})")
	void insertTempAtms(@Param("value") Integer value);
	
	@Delete("DELETE FROM T_CM_TEMP_ATM_LIST ")
	void deleteTempAtms();
	
	@Insert("INSERT INTO T_CM_TEMP_ATM_GROUP_LIST VALUES(#{value})")
	void insertTempAtmGroups(@Param("value") Integer value);
	
	@Insert("INSERT INTO T_CM_TEMP_ATM_GROUP_LIST VALUES(#{item})")
	void insertTempAtmGroupsIds(@Param("item") Integer item);

	@Delete("DELETE FROM T_CM_TEMP_ATM_GROUP_LIST ")
	void deleteTempAtmGroups();
}
