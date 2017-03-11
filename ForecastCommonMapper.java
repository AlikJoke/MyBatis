package ru.bpc.cm.forecasting.orm;

import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Select;

import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.forecasting.controllers.ForecastCommonController;
import ru.bpc.cm.utils.ObjectPair;

/**
 * Интерфейс-маппер для класса {@link ForecastCommonController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 04.03.2017
 * @version 1.0.0
 *
 */
public interface ForecastCommonMapper extends IMapper {

	@ConstructorArgs({
		@Arg(column = "VALUE", javaType = Double.class),
		@Arg(column = "VALUE", javaType = String.class)
	})
	@ResultType(ObjectPair.class)
	@Select("select aga.value from T_CM_ATM2ATM_GROUP agr join T_CM_ATM_GROUP ag on (ag.id = agr.atm_group_id) "
			+ "left outer join T_CM_ATM_GROUP_ATTR aga on (agr.atm_group_id = aga.atm_group_id) where 1=1 "
			+ "AND aga.attr_id > #{attrId} AND ag.type_id = #{groupId} "
			+ "AND agr.ATM_ID = #{atmId} ORDER BY aga.attr_id")
	List<ObjectPair<Double, String>> getEncCostPercents(@Param("attrId") Integer attrId, @Param("groupId") Integer groupId,
			@Param("atmId") Integer atmId);
	
	@ConstructorArgs({
		@Arg(column = "DATE_FORTHCOMING_ENCASHMENT", javaType = Timestamp.class),
		@Arg(column = "ENC_COUNT", javaType = Integer.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT trunc(DATE_FORTHCOMING_ENCASHMENT) as DATE_FORTHCOMING_ENCASHMENT, count(distinct ENC_PLAN_ID) as ENC_COUNT "
			+ "FROM T_CM_ENC_PLAN WHERE 1=1 AND DATE_FORTHCOMING_ENCASHMENT >  #{startDate} "
			+ "AND DATE_FORTHCOMING_ENCASHMENT <= #{endDate} AND ATM_ID IN (SELECT ATM_ID "
			+ "FROM T_CM_ATM2ATM_GROUP WHERE ATM_GROUP_ID = #{atmCount}) GROUP BY trunc(DATE_FORTHCOMING_ENCASHMENT) ")
	List<ObjectPair<Timestamp, Integer>> getEncMapDaysPlan(@Param("startDate") Timestamp startDate,
			@Param("endDate") Timestamp endDate, @Param("atmCount") Integer atmCount);
	
	@ConstructorArgs({
		@Arg(column = "DATE_FORTHCOMING_ENCASHMENT", javaType = Timestamp.class),
		@Arg(column = "ENC_COUNT", javaType = Integer.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT trunc(DATE_FORTHCOMING_ENCASHMENT) as DATE_FORTHCOMING_ENCASHMENT, count(distinct ID) as ENC_COUNT "
			+ "FROM T_CM_ENC_PERIOD WHERE 1=1 AND DATE_FORTHCOMING_ENCASHMENT >  #{startDate} "
			+ "AND DATE_FORTHCOMING_ENCASHMENT <= #{endDate} AND ATM_ID IN (SELECT ATM_ID "
			+ "FROM T_CM_ATM2ATM_GROUP WHERE ATM_GROUP_ID = #{atmCount}) GROUP BY trunc(DATE_FORTHCOMING_ENCASHMENT) ")
	List<ObjectPair<Timestamp, Integer>> getEncMapDaysPeriod(@Param("startDate") Timestamp startDate,
			@Param("endDate") Timestamp endDate, @Param("atmCount") Integer atmCount);

	@ConstructorArgs({
		@Arg(column = "DATE_FORTHCOMING_ENCASHMENT", javaType = Timestamp.class),
		@Arg(column = "ENC_COUNT", javaType = Integer.class)
	})
	@ResultType(ObjectPair.class)
	@Select("SELECT trunc(DATE_FORTHCOMING_ENCASHMENT) as DATE_FORTHCOMING_ENCASHMENT, count(distinct ID) as ENC_COUNT "
			+ "FROM T_CM_ENC_COMPARE WHERE 1=1 AND DATE_FORTHCOMING_ENCASHMENT >  #{startDate} "
			+ "AND DATE_FORTHCOMING_ENCASHMENT <= #{endDate} AND ATM_ID IN (SELECT ATM_ID "
			+ "FROM T_CM_ATM2ATM_GROUP WHERE ATM_GROUP_ID = #{atmCount}) GROUP BY trunc(DATE_FORTHCOMING_ENCASHMENT) ")
	List<ObjectPair<Timestamp, Integer>> getEncMapDaysCompare(@Param("startDate") Timestamp startDate,
			@Param("endDate") Timestamp endDate, @Param("atmCount") Integer atmCount);
	
	@Result(column = "DAYS", javaType = Integer.class)
	@ResultType(Integer.class)
	@Select("SELECT count(CL_DATE) as DAYS from T_CM_CALENDAR_DAYS ac WHERE ac.cl_day_type = #{dayType} "
			+ "and ac.CL_ID = (SELECT CALENDAR_ID FROM t_cm_atm where atm_id = #{atmId} ) and ac.CL_DATE > #{start} "
			+ "and ac.CL_DATE <= #{finish} ")
	Integer getPeriodAvailableDates(@Param("dayType") Integer dayType, @Param("atmId") Integer atmId,
			@Param("start") Timestamp startDate, @Param("finish") Timestamp endDate);

	@Result(column = "CL_DATE", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("SELECT CL_DATE from T_CM_CALENDAR_DAYS ac "
			+ "WHERE ac.cl_id = (SELECT CALENDAR_ID FROM t_cm_atm where atm_id = #{atmId} ) and ac.cl_day_type = #dayType{} "
			+ "and ac.CL_DATE > #{start} and ac.CL_DATE <= #{finish} ")
	List<Timestamp> getAtmNotAvailableDates(@Param("atmId") Integer atmId, @Param("dayType") Integer dayType,
			@Param("start") Timestamp startDate, @Param("finish") Timestamp endDate);
	
	@Result(column = "CL_DATE", javaType = Timestamp.class)
	@ResultType(Timestamp.class)
	@Select("SELECT CL_DATE from T_CM_CALENDAR_DAYS ac "
			+ "WHERE ac.cl_id = (SELECT CALENDAR_ID FROM t_cm_atm where atm_id = #{atmId} ) and ac.CL_DAY_ENC_AVAIL = 0 "
			+ "and ac.CL_DATE >= #{start} and ac.CL_DATE <= #{finish} ")
	List<Timestamp> getEncNotAvailableDates(@Param("atmId") Integer atmId, @Param("start") Timestamp startDate,
			@Param("finish") Timestamp endDate);
}
