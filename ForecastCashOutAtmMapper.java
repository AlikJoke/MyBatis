package ru.bpc.cm.forecasting.orm;

import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Select;

import ru.bpc.cm.config.IMapper;
import ru.bpc.cm.forecasting.controllers.ForecastCashOutAtmController;

/**
 * Интерфейс-маппер для класса {@link ForecastCashOutAtmController}.
 * 
 * @author Alimurad A. Ramazanov
 * @since 23.02.2017
 * @version 1.0.0
 *
 */
public interface ForecastCashOutAtmMapper extends IMapper {

	@Result(column = "SUMM", javaType = Double.class)
	@Select("SELECT CURR_REMAINING AS SUMM FROM V_CM_CASHOUT_CURR_ACT_REM WHERE ATM_ID = #{atmId} AND "
			+ " CURR_CODE = #{curr} AND ENCASHMENT_ID = #{encId} AND STAT_DATE <= #{startDate} "
			+ " ORDER BY STAT_DATE desc,ENCASHMENT_ID desc")
	List<Double> getCurrRemaining(@Param("atmId") Integer atmId, @Param("curr") Integer curr, @Param("encId") Integer encId,
			@Param("startDate") Timestamp startDate);
}
