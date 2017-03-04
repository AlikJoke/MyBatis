package ru.bpc.cm.forecasting.orm;

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
import org.apache.ibatis.annotations.SelectProvider;

import ru.bpc.cm.cashmanagement.orm.builders.ForecastNominalsBuilder;
import ru.bpc.cm.cashmanagement.orm.items.NominalCountItem;
import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.forecasting.controllers.ForecastNominalsController;
import ru.bpc.cm.items.forecast.nominal.NominalItem;
import ru.bpc.cm.utils.ObjectPair;

/**
 * Интерфейс-маппер для класса {@link ForecastNominalsController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 04.03.2017
 * @version 1.0.0
 *
 */
public interface ForecastNominalsMapper extends IMapper {

	@ConstructorArgs({
		@Arg(column = "DENOM_VALUE", javaType = Integer.class),
		@Arg(column = "COUNT_TRANS", javaType = Integer.class),
		@Arg(column = "COUNT_DAYS", javaType = Double.class),
		@Arg(column = "DENOM_COUNT", javaType = Double.class),
		@Arg(column = "DENOM_CURR", javaType = Integer.class),
	})
	@SelectProvider(type = ForecastNominalsBuilder.class, method = "getCurrNominalsBuilder")
	@Options(useCache = true, fetchSize = 1000)
	@ResultType(NominalItem.class)
	List<NominalItem> getCurrNominals(@Param("encList") List<Integer> encList, @Param("curr") Integer curr,
			@Param("atmId") Integer atmId);
	
	@Results({
		@Result(column = "CASS_NUMBER", property = "cassNum", javaType = Integer.class),
		@Result(column = "CASS_VALUE", property = "denom", javaType = Integer.class),
	})
	@Select("SELECT CASS_NUMBER,CASS_VALUE FROM T_CM_ATM_CASSETTES WHERE ATM_ID = #{atmId} AND CASS_CURR = #{curr} "
			+ "AND CASS_TYPE = #{cassTypeId}")
	@Options(useCache = true, fetchSize = 1000)
	@ResultType(NominalItem.class)
	List<NominalItem> getCoCurrNominalsFromAtmCassettes(@Param("atmId") Integer atmId, @Param("curr") Integer curr,
			@Param("cassTypeId") Integer cassTypeId);
	
	@ConstructorArgs({
		@Arg(column = "COUNT_TRANS", javaType = Integer.class),
		@Arg(column = "DENOM_COUNT", javaType = Integer.class),
		@Arg(column = "COUNT_DAYS", javaType = Double.class)
	})
	@SelectProvider(type = ForecastNominalsBuilder.class, method = "getCoCurrNominalsFromAtmCassettesBuilder_count")
	@Options(useCache = true, fetchSize = 1000)
	@ResultType(NominalCountItem.class)
	List<NominalCountItem> getCoCurrNominalsFromAtmCassettes_count(@Param("encList") List<Integer> encList,
			@Param("curr") Integer curr, @Param("atmId") Integer atmId, @Param("denom") Integer denom);
	
	@Results({
		@Result(column = "CASS_NUMBER", property = "cassNum", javaType = Integer.class),
		@Result(column = "CASS_VALUE", property = "denom", javaType = Integer.class),
	})
	@Select("SELECT CASS_NUMBER,CASS_VALUE FROM T_CM_ATM_CASSETTES WHERE ATM_ID = #{atmId} "
			+ "AND CASS_CURR = #{curr} AND CASS_TYPE = #{cassTypeId}")
	@Options(useCache = true, fetchSize = 1000)
	@ResultType(NominalItem.class)
	List<NominalItem> getCrCurrNominalsFromAtmCassettes(@Param("atmId") Integer atmId, @Param("curr") Integer curr,
			@Param("cassTypeId") Integer cassTypeId);
	
	@ConstructorArgs({
		@Arg(column = "COUNT_TRANS", javaType = Integer.class),
		@Arg(column = "DENOM_COUNT", javaType = Integer.class),
		@Arg(column = "COUNT_DAYS", javaType = Double.class)
	})
	@SelectProvider(type = ForecastNominalsBuilder.class, method = "getCrCurrNominalsFromAtmCassettesBuilder_count")
	@Options(useCache = true, fetchSize = 1000)
	@ResultType(NominalCountItem.class)
	List<NominalCountItem> getCrCurrNominalsFromAtmCassettes_count(@Param("encList") List<Integer> encList,
			@Param("curr") Integer curr, @Param("atmId") Integer atmId, @Param("denom") Integer denom);

	@Result(column = "SUMM", javaType = Double.class)
	@ResultType(Double.class)
	@Select("select sum(curr_summ)/count(distinct stat_date) as SUMM from (select cs.stat_date,  "
			+ "case COALESCE(acd.COEFF,1) when 0 then 0 else cs.denom_count/COALESCE(acd.COEFF,1) END as curr_summ  "
			+ "from V_CM_CASHOUT_DENOM_STAT cs join t_cm_atm a on (cs.ATM_ID = a.ATM_ID) "
			+ "left outer join t_cm_calendar_days cd on (cs.STAT_DATE = cd.CL_DATE and cd.CL_ID = a.CALENDAR_ID) "
			+ "left outer join T_CM_ATM_CALENDAR_DAYS acd on (cs.STAT_DATE = acd.CL_DATE "
			+ "and acd.CURRENCY = cs.DENOM_CURR and acd.ATM_ID = a.ATM_ID) where 1=1 and cs.atm_id = #{atmId} "
			+ "and DENOM_CURR = #{curr} and (to_char(cs.stat_date,'hh24') < #{dayEnd}  "
			+ "and to_char(cs.stat_date,'hh24') >= #{dayStart} ) and COALESCE(cd.CL_DAY_TYPE,0) != #{dayTypeId} "
			+ "and COALESCE(acd.USER_DISABLED,0) != 1 and cs.stat_date <= #{endDate}  and cs.stat_date > #{startDate} "
			+ "and COALESCE(acd.CURRENCY_MODE, #{mode}) = #{mode} and cs.denom_value = #{denom}) ")
	Double getAvgDenomDemandStat(@Param("atmId") Integer atmId, @Param("curr") Integer curr,
			@Param("dayEnd") Integer dayEnd, @Param("dayStart") Integer dayStart, @Param("dayTypeId") Integer dayTypeId,
			@Param("endDate") Timestamp endDate, @Param("startDate") Timestamp startDate, @Param("mode") String mode,
			@Param("denom") Integer denom);
	
	@Result(column = "DENOM", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT MIN(DENOM) as DENOM FROM T_CM_CURR_DENOM WHERE CURR_CODE = #{curr} AND DENOM > #{denom}")
	Integer changeNominals(@Param("curr") Integer curr, @Param("denom") Integer denom);
	
	@ConstructorArgs({
		@Arg(column = "SUMM", javaType = Double.class),
		@Arg(column = "STAT_DATE", javaType = Timestamp.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT DENOM_REMAINING AS SUMM,STAT_DATE FROM V_CM_CASHOUT_DENOM_STAT WHERE ATM_ID = #{atmId} "
			+ " AND DENOM_CURR = #{curr} AND ENCASHMENT_ID = #{encId} AND STAT_DATE <= #{startDate} AND DENOM_VALUE = #{denom} "
			+ " ORDER BY STAT_DATE desc,ENCASHMENT_ID desc")
	ObjectPair<Double, Timestamp> getDenomRemaining_withStatDate(@Param("atmId") Integer atmId,
			@Param("curr") Integer curr, @Param("encId") Integer encId, @Param("startDate") Timestamp startDate,
			@Param("denom") Integer denom);

	@Result(column = "SUMM", javaType = Double.class)
	@ResultType(Double.class)
	@Select("select SUM(cs.CASS_REMAINING) as SUMM from T_CM_CASHOUT_CASS_STAT cs where cs.atm_id = #{atmId} "
			+ "AND cs.cass_curr = #{curr} AND cs.encashment_id = #{encId} AND cs.stat_date = #{startDate} AND cs.cass_state != 0 "
			+ "AND cs.CASS_VALUE = #{denom}")
	Double getDenomRemaining(@Param("atmId") Integer atmId, @Param("curr") Integer curr, @Param("encId") Integer encId,
			@Param("startDate") Timestamp startDate, @Param("denom") Integer denom);
}
