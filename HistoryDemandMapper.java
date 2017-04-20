package ru.bpc.cm.forecasting.orm;

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
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.Update;

import ru.bpc.cm.cashmanagement.orm.builders.HistoryDemandBuilder;
import ru.bpc.cm.cashmanagement.orm.handlers.DisabledCNTTypeHandler;
import ru.bpc.cm.cashmanagement.orm.handlers.SqlDateToJavaDateHandler;
import ru.bpc.cm.cashmanagement.orm.handlers.TimestampToHourTypeHandler;
import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.forecasting.controllers.HistoryDemandController;
import ru.bpc.cm.items.monitoring.DemandCompareItem;
import ru.bpc.cm.items.monitoring.HourDemandCompareItem;

/**
 * Интерфейс-маппер для класса {@link HistoryDemandController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 23.02.2017
 * @version 1.0.0
 *
 */
public interface HistoryDemandMapper extends IMapper {

	@Results({
		@Result(column = "ATM_ID", property = "atmId", javaType = Integer.class),
		@Result(column = "STAT_DATE", property = "day", typeHandler = SqlDateToJavaDateHandler.class, javaType = java.util.Date.class),
		@Result(column = "STAT_SUMM", property = "statSumm", javaType = Integer.class),
		@Result(column = "FORECASTED_SUMM", property = "forecastedSumm", javaType = Integer.class),
		@Result(column = "DISABLED_CNT", property = "disabled", typeHandler = DisabledCNTTypeHandler.class, javaType = Boolean.class)
	})
	@SelectProvider(type = HistoryDemandBuilder.class, method = "getDemandCompareForAtmBuilder")
	@Options(useCache = true, fetchSize = 1000)
	List<DemandCompareItem> getDemandCompareForAtm(@Param("statViewName") String statViewName,
			@Param("atmId") String atmId, @Param("dateFrom") Date dateFrom, @Param("dateTo") Date dateTo,
			@Param("curr") Integer curr, @Param("modeId") Integer modeId);

	@ConstructorArgs({
		@Arg(column = "STAT_DATE", typeHandler = TimestampToHourTypeHandler.class, javaType = Integer.class),
		@Arg(column = "STAT_SUMM", javaType = Integer.class),
		@Arg(column = "FORECASTED_SUMM", javaType = Integer.class),
		@Arg(column = "DISABLED_CNT", typeHandler = DisabledCNTTypeHandler.class, javaType = Boolean.class)
	})
	@SelectProvider(type = HistoryDemandBuilder.class, method = "getDemandHourCompareForAtmBuilder")
	@Options(useCache = true, fetchSize = 1000)
	List<HourDemandCompareItem> getDemandHourCompareForAtm(@Param("statViewName") String statViewName,
			@Param("atmId") String atmId, @Param("day") Date day, @Param("curr") Integer curr,
			@Param("modeId") Integer modeId);
	
	@Delete("DELETE FROM T_CM_ATM_DEMAND_HISTORY WHERE ATM_ID = #{atmId} AND CL_DATE >= #{statsEndDate}")
	void deleteAtmCalendarDays(@Param("atmId") Integer atmId, @Param("statsEndDate") Date statsEndDate);

	@Insert("INSERT INTO T_CM_ATM_DEMAND_HISTORY (ATM_ID, CURRENCY, CL_DATE, CURRENCY_MODE, DEMAND) "
			+ "VALUES(#{atmId}, #{curr}, #{day}, #{modeId}, #{demand})")
	void saveAtmCalendarDays(@Param("atmId") Integer atmId, @Param("curr") Integer curr, @Param("day") Timestamp day,
			@Param("modeId") Integer modeId, @Param("demand") Double demand);

	@Update("UPDATE T_CM_ATM_CALENDAR_DAYS SET USER_DISABLED = #{disabled} WHERE ATM_ID = #{atmId} "
			+ "AND CL_DATE >= #{day}" + "AND CL_DATE < #{calTime} " + "AND CURRENCY = #{curr} "
			+ "AND CURRENCY_MODE = #{modeId}")
	void changeAtmCalendarDay(@Param("disabled") Integer disabled, @Param("atmId") String atmId, @Param("day") Date day,
			@Param("calTime") Date calTime, @Param("curr") Integer curr, @Param("modeId") Integer modeId);

	@Update("UPDATE T_CM_ATM_CALENDAR_DAYS SET USER_DISABLED = #{disabled} WHERE ATM_ID = #{atmId} "
			+ "AND CL_DATE = #{calTime} AND CURRENCY = #{curr} AND CURRENCY_MODE = #{modeId}")
	void changeAtmCalendarHour(@Param("disabled") Integer disabled, @Param("atmId") String atmId,
			@Param("calTime") Date calTime, @Param("curr") Integer curr, @Param("modeId") Integer modeId);
}
