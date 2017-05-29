package ru.bpc.cm.reports.orm;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;

import ru.bpc.cm.items.reports.RepCurrStatItem;
import ru.bpc.cm.items.reports.RepCurrStatRecItem;
import ru.bpc.cm.items.reports.RepDenomStatItem;
import ru.bpc.cm.items.reports.RepDenomStatRecItem;
import ru.bpc.cm.orm.common.IMapper;
import ru.bpc.cm.orm.handlers.SqlDateToJavaDateHandler;
import ru.bpc.cm.reports.orm.builders.ReportDenomCurrBuilder;

/**
 * Интерфейс-маппер для класса ReportsDenomCurrController.
 * 
 * @author Alimurad A. Ramazanov
 * @since 29.04.2017
 * @version 1.0.1
 *
 */
public interface ReportsDenomCurrMapper extends IMapper {

	@Results({ 
			@Result(column = "ATM_ID", property = "atmID", javaType = Integer.class),
			@Result(column = "ENC_DATE_FROM", property = "encDateFrom", javaType = Date.class, typeHandler = SqlDateToJavaDateHandler.class),
			@Result(column = "CURR_CODE", property = "currCode", javaType = Integer.class),
			@Result(column = "CODE_A3", property = "currCodeA3", javaType = String.class),
			@Result(column = "LOADED", property = "loaded", javaType = Integer.class),
			@Result(column = "WITHDRAWAL", property = "withdrawal", javaType = Integer.class),
			@Result(column = "REMAINING", property = "remaining", javaType = Integer.class),
			@Result(column = "LEFT_BEFORE_CASH_ADD", property = "leftBeforeCashAdd", javaType = Integer.class),
			@Result(column = "TRANS_COUNT", property = "transCount", javaType = Integer.class),
			@Result(column = "CURR_COUNT", property = "currCount", javaType = Integer.class),
			@Result(column = "LINE_NUBMER", property = "lineNumber", javaType = Integer.class),
			@Result(column = "summTrans", property = "AVERAGE_TRANSACTION_SUM", javaType = Integer.class)
	})
	@Select("SELECT v.ATM_ID, v.CURR_CODE, ci.code_a3 as CODE_A3, v.DATE_FROM as ENC_DATE_FROM, "
			+ "v.DATE_TO as ENC_DATE_TO, v.LOADED as LOADED, "
			+ "COALESCE(v.LEFT_BEFORE_CASH_ADD,0) as LEFT_BEFORE_CASH_ADD, "
			+ "COALESCE(v.UNLOADED,COALESCE(v.LEFT_BEFORE_CASH_ADD,0)+v.LOADED - sum(v.CURR_SUMM)) as REMAINING, "
			+ "sum(v.CURR_SUMM) as WITHDRAWAL, SUM(v.curr_trans_count) as TRANS_COUNT, "
			+ "CASE WHEN SUM(v.curr_trans_count) = 0 THEN 0 ELSE SUM(v.curr_summ)/SUM(v.curr_trans_count) "
			+ "END as AVERAGE_TRANSACTION_SUM, "
			+ "count(distinct v.CURR_CODE) over (partition by v.ATM_ID,v.DATE_FROM) as CURR_COUNT, "
			+ "row_number() over (partition by  v.ATM_ID,v.DATE_FROM order by v.CURR_CODE desc) as LINE_NUBMER "
			+ "FROM v_cm_r_curr_stat v,t_cm_temp_atm_list tal, t_cm_curr ci where 1=1 "
			+ "AND v.atm_id = tal.id AND ci.code_n3 = v.CURR_CODE AND v.date_from < #{dateTo} AND v.date_from > #{dateFrom} "
			+ "GROUP BY  v.ATM_ID,v.DATE_FROM,v.DATE_TO,v.CURR_CODE, ci.code_a3, v.ENCASHMENT_ID,v.LOADED,v.UNLOADED,v.LEFT_BEFORE_CASH_ADD "
			+ "ORDER BY ATM_ID,v.DATE_FROM desc,v.CURR_CODE ")
	@ResultType(RepCurrStatItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepCurrStatItem> getReportCoCurrStat(@Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom);
	
	@Results({ 
			@Result(column = "ATM_ID", property = "atmID", javaType = Integer.class),
			@Result(column = "ENC_DATE_FROM", property = "encDateFrom", javaType = Date.class, typeHandler = SqlDateToJavaDateHandler.class),
			@Result(column = "CURR_CODE", property = "currCode", javaType = Integer.class),
			@Result(column = "CODE_A3", property = "currCodeA3", javaType = String.class),
			@Result(column = "LOADED", property = "loaded", javaType = Integer.class),
			@Result(column = "CURR_SUMM_IN", property = "currSummIn", javaType = Integer.class),
			@Result(column = "CURR_SUMM_OUT", property = "currSummOut", javaType = Integer.class),
			@Result(column = "REMAINING", property = "remaining", javaType = Integer.class),
			@Result(column = "LEFT_BEFORE_CASH_ADD", property = "leftBeforeCashAdd", javaType = Integer.class),
			@Result(column = "TRANS_COUNT_IN", property = "transCountIn", javaType = Integer.class),
			@Result(column = "TRANS_COUNT_OUT", property = "transCountOut", javaType = Integer.class),
			@Result(column = "CURR_COUNT", property = "currCount", javaType = Integer.class),
			@Result(column = "LINE_NUBMER", property = "lineNumber", javaType = Integer.class),
			@Result(column = "AVERAGE_TRANSACTION_SUM_IN", property = "summTransIn", javaType = Integer.class),
			@Result(column = "AVERAGE_TRANSACTION_SUM_OUT", property = "summTransOut", javaType = Integer.class)
	})
	@Select("SELECT v.ATM_ID, v.CURR_CODE, ci.code_a3 as CODE_A3, v.DATE_FROM as ENC_DATE_FROM, "
			+ "v.DATE_TO as ENC_DATE_TO, v.LOADED, v.UNLOADED as REMAINING, "
			+ "sum(v.CURR_SUMM_IN) as CURR_SUMM_IN, sum(v.CURR_SUMM_OUT) as CURR_SUMM_OUT, "
			+ "SUM(v.curr_trans_count_in) as TRANS_COUNT_IN, SUM(v.curr_trans_count_out) as TRANS_COUNT_OUT, "
			+ "CASE WHEN SUM(v.curr_trans_count_in) = 0 THEN 0 "
			+ "ELSE SUM(v.curr_summ_in)/SUM(v.curr_trans_count_in) END as AVERAGE_TRANSACTION_SUM_IN, "
			+ "CASE WHEN SUM(v.curr_trans_count_out) = 0 THEN 0 "
			+ "ELSE SUM(v.curr_summ_out)/SUM(v.curr_trans_count_out) END as AVERAGE_TRANSACTION_SUM_OUT, "
			+ "count(distinct v.CURR_CODE) over (partition by v.ATM_ID,v.DATE_FROM) as CURR_COUNT, "
			+ "row_number() over (partition by  v.ATM_ID,v.DATE_FROM order by v.CURR_CODE desc) as LINE_NUBMER "
			+ "FROM v_cm_r_cashin_r_curr_stat v,t_cm_temp_atm_list tal, t_cm_curr ci where 1=1 "
			+ "AND v.atm_id = tal.id AND ci.code_n3 = v.CURR_CODE AND v.date_from < #{dateTo} AND v.date_from > #{dateFrom} "
			+ "GROUP BY  v.ATM_ID,v.DATE_FROM,v.DATE_TO,v.CURR_CODE, ci.code_a3, v.CASH_IN_ENCASHMENT_ID,v.LOADED,v.UNLOADED "
			+ "ORDER BY ATM_ID,v.DATE_FROM desc,v.CURR_CODE")
	@ResultType(RepCurrStatRecItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepCurrStatRecItem> getReportCrCurrStat(@Param("dateTo") Timestamp dateTo,
			@Param("dateFrom") Timestamp dateFrom);
	
	@Results({ 
			@Result(column = "ATM_ID", property = "atmID", javaType = Integer.class),
			@Result(column = "ENC_DATE_FROM", property = "encDateFrom", javaType = Timestamp.class),
			@Result(column = "ENC_DATE_TO", property = "encDateFrom", javaType = Timestamp.class),
			@Result(column = "DENOM_CURR_A3", property = "denomCurrCodeA3", javaType = String.class),
			@Result(column = "DENOM_VALUE", property = "denomValue", javaType = Integer.class),
			@Result(column = "DENOM_IN", property = "denomCountIn", javaType = Integer.class),
			@Result(column = "DENOM_OUT", property = "denomCountOut", javaType = Integer.class),
			@Result(column = "DENOM_TRANS_COUNT", property = "transCount", javaType = Integer.class),
			@Result(column = "CURR_TRANS_COUNT", property = "transCoeff", javaType = Double.class),
			@Result(column = "DENOM_COUNT", property = "denomCount", javaType = Integer.class),
			@Result(column = "LINE_NUBMER", property = "lineNumber", javaType = Integer.class)
	})
	@SelectProvider(type = ReportDenomCurrBuilder.class, method = "getReportCoDenomStatBuilder")
	@ResultType(RepDenomStatItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepDenomStatItem> getReportCoDenomStat(@Param("splitInCycles") boolean splitInCycles,
			@Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom);
	
	@Results({ 
			@Result(column = "ATM_ID", property = "atmID", javaType = Integer.class),
			@Result(column = "ENC_DATE_FROM", property = "encDateFrom", javaType = Timestamp.class),
			@Result(column = "ENC_DATE_TO", property = "encDateFrom", javaType = Timestamp.class),
			@Result(column = "DENOM_CURR_A3", property = "denomCurrCodeA3", javaType = String.class),
			@Result(column = "DENOM_VALUE", property = "denomValue", javaType = Integer.class),
			@Result(column = "DENOM_COUNT_IN", property = "denomCountIn", javaType = Integer.class),
			@Result(column = "DENOM_COUNT_OUT", property = "denomCountOut", javaType = Integer.class),
			@Result(column = "DENOM_TRANS_COUNT", property = "transCount", javaType = Integer.class),
			@Result(column = "CURR_TRANS_COUNT", property = "transCoeff", javaType = Double.class),
			@Result(column = "DENOM_COUNT", property = "denomCount", javaType = Integer.class),
			@Result(column = "LINE_NUBMER", property = "lineNumber", javaType = Integer.class)
	})
	@SelectProvider(type = ReportDenomCurrBuilder.class, method = "getReportCoDenomStatForAtmBuilder")
	@ResultType(RepDenomStatItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepDenomStatItem> getReportCoDenomStatForAtm(@Param("splitInCycles") boolean splitInCycles,
			@Param("atmId") Integer atmId, @Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom);
	
	@Results({ 
			@Result(column = "ENC_DATE_FROM", property = "encDateFrom", javaType = Timestamp.class),
			@Result(column = "ENC_DATE_TO", property = "encDateFrom", javaType = Timestamp.class),
			@Result(column = "DENOM_CURR_A3", property = "denomCurrCodeA3", javaType = String.class),
			@Result(column = "DENOM_VALUE", property = "denomValue", javaType = Integer.class),
			@Result(column = "DENOM_COUNT_IN", property = "denomCountIn", javaType = Integer.class),
			@Result(column = "DENOM_COUNT_OUT", property = "denomCountOut", javaType = Integer.class),
			@Result(column = "DENOM_TRANS_COUNT_IN", property = "transCountIn", javaType = Integer.class),
			@Result(column = "DENOM_TRANS_COUNT_OUT", property = "transCountOut", javaType = Integer.class),
			@Result(column = "LOADED", property = "denomLoaded", javaType = Integer.class),
			@Result(column = "CURR_TRANS_COUNT_IN", property = "transCoeffIn", javaType = Double.class),
			@Result(column = "CURR_TRANS_COUNT_OUT", property = "transCoeffOut", javaType = Double.class),
			@Result(column = "DENOM_COUNT", property = "denomCount", javaType = Integer.class),
			@Result(column = "LINE_NUBMER", property = "lineNumber", javaType = Integer.class)
	})
	@SelectProvider(type = ReportDenomCurrBuilder.class, method = "getReportCrDenomStatBuilder")
	@ResultType(RepDenomStatRecItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepDenomStatRecItem> getReportCrDenomStat(@Param("splitInCycles") boolean splitInCycles,
			@Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom);
	
	@Results({ 
			@Result(column = "ENC_DATE_FROM", property = "encDateFrom", javaType = Timestamp.class),
			@Result(column = "ENC_DATE_TO", property = "encDateFrom", javaType = Timestamp.class),
			@Result(column = "DENOM_CURR_A3", property = "denomCurrCodeA3", javaType = String.class),
			@Result(column = "DENOM_VALUE", property = "denomValue", javaType = Integer.class),
			@Result(column = "DENOM_COUNT_IN", property = "denomCountIn", javaType = Integer.class),
			@Result(column = "DENOM_COUNT_OUT", property = "denomCountOut", javaType = Integer.class),
			@Result(column = "DENOM_TRANS_COUNT_IN", property = "transCountIn", javaType = Integer.class),
			@Result(column = "DENOM_TRANS_COUNT_OUT", property = "transCountOut", javaType = Integer.class),
			@Result(column = "LOADED", property = "denomLoaded", javaType = Integer.class),
			@Result(column = "CURR_TRANS_COUNT_IN", property = "transCoeffIn", javaType = Double.class),
			@Result(column = "CURR_TRANS_COUNT_OUT", property = "transCoeffOut", javaType = Double.class),
			@Result(column = "DENOM_COUNT", property = "denomCount", javaType = Integer.class),
			@Result(column = "LINE_NUBMER", property = "lineNumber", javaType = Integer.class)
	})
	@SelectProvider(type = ReportDenomCurrBuilder.class, method = "getReportCrDenomStatForAtmBuilder")
	@ResultType(RepDenomStatRecItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepDenomStatRecItem> getReportCrDenomStatForAtm(@Param("splitInCycles") boolean splitInCycles,
			@Param("atmId") Integer atmId, @Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom);
}
