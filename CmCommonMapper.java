package ru.bpc.cm.cashmanagement.orm;

import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Select;

import ru.bpc.cm.cashmanagement.CmCommonController;
import ru.bpc.cm.cashmanagement.orm.items.CodesItem;
import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.utils.ObjectPair;

/**
 * Интерфейс-маппер для класса {@link CmCommonController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 24.02.2017
 * @version 1.0.0
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
	
	@ConstructorArgs({
		@Arg(column = "ATTR_ID", javaType = Integer.class),
		@Arg(column = "VALUE", javaType = String.class)
	})
	@Select("select aga.attr_id ,aga.value from T_CM_ATM2ATM_GROUP agr "
			+ "join T_CM_ATM_GROUP ag on (ag.id = agr.atm_group_id) left outer join T_CM_ATM_GROUP_ATTR aga on "
			+ "(agr.atm_group_id = aga.atm_group_id) where 1=1 AND ag.type_id != #{typeId} "
			+ "AND agr.ATM_ID = #{atmId} ")
	@Options(useCache = true, fetchSize = 1000)
	ObjectPair<Integer, String> getAtmAttributes(@Param("typeId") Integer typeId, @Param("atmId") Integer atmId);
	
	@ConstructorArgs({ 
		@Arg(column = "MULTIPLE_FLAG", javaType = String.class),
		@Arg(column = "CNVT_RATE", javaType = Double.class) 
	})
	@Select("SELECT CNVT_RATE, MULTIPLE_FLAG FROM T_CM_CURR_CONVERT_RATE WHERE DEST_CURR_CODE = #{destCurr} "
			+ "AND SRC_CURR_CODE = #{srcCurr} AND DEST_INST_ID = #{instId} AND SRC_INST_ID = #{instId} "
			+ "ORDER BY CNVT_DATE DESC")
	ObjectPair<String, Double> convertValue(@Param("destCurr") Integer destCurr, @Param("srcCurr") Integer srcCurr,
			@Param("instId") String instId);
	
	
}
