package ru.bpc.cm.reports.orm;

import java.sql.Timestamp;
import java.util.List;

import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.SelectProvider;

import ru.bpc.cm.items.reports.RepCurrWdwlRemItem;
import ru.bpc.cm.items.reports.RepCurrWdwlRemRecItem;
import ru.bpc.cm.items.reports.RepDenomWdwlRemItem;
import ru.bpc.cm.items.reports.RepDenomWdwlRemRecItem;
import ru.bpc.cm.items.reports.ReportFilter;
import ru.bpc.cm.orm.common.IMapper;
import ru.bpc.cm.orm.handlers.DoubleToLongHandler;
import ru.bpc.cm.reports.orm.builders.ReportsWdwlRemBuilder;

/**
 * Интерфейс-маппер для класса ReportsWdwlRemController.
 * 
 * @author Alimurad A. Ramazanov
 * @since 29.04.2017
 * @version 1.0.0
 *
 */
public interface ReportsWdwlRemMapper extends IMapper {

	@Results({
			@Result(column = "ATM_ID", property = "atmID", javaType = Integer.class),
			@Result(column = "CURR_CODE", property = "currCode", javaType = Integer.class),
			@Result(column = "STAT_DATE", property = "statDate", javaType = Timestamp.class),
			@Result(column = "CODE_A3", property = "currCodeA3", javaType = String.class),
			@Result(column = "CURR_SUMM", property = "currSumm", javaType = Double.class, typeHandler = DoubleToLongHandler.class),
			@Result(column = "CURR_REMAINING", property = "currRemaining", javaType = Double.class, typeHandler = DoubleToLongHandler.class),
			@Result(column = "ENCASHMENT_ID", property = "encashmentId", javaType = Integer.class),
	})
	@SelectProvider(type = ReportsWdwlRemBuilder.class, method = "getReportCoCurrWithdrawalRemainAtmBuilder")
	@ResultType(RepCurrWdwlRemItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepCurrWdwlRemItem> getReportCoCurrWithdrawalRemainAtm(@Param("filter") ReportFilter filter,
			@Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom,
			@Param("wdwlRemCurrCode") Integer wdwlRemCurrCode, @Param("atmId") Integer atmId,
			@Param("nameAndAddr") String nameAndAddr);

	@Results({
		@Result(column = "CURR_CODE", property = "currCode", javaType = Integer.class),
		@Result(column = "STAT_DATE", property = "statDate", javaType = Timestamp.class),
		@Result(column = "CODE_A3", property = "currCodeA3", javaType = String.class),
		@Result(column = "CURR_SUMM", property = "currSumm", javaType = Double.class, typeHandler = DoubleToLongHandler.class),
		@Result(column = "CURR_REMAINING", property = "currRemaining", javaType = Double.class, typeHandler = DoubleToLongHandler.class)
	})
	@SelectProvider(type = ReportsWdwlRemBuilder.class, method = "getReportCoCurrWithdrawalRemainGroupBuilder")
	@ResultType(RepCurrWdwlRemItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepCurrWdwlRemItem> getReportCoCurrWithdrawalRemainGroup(@Param("filter") ReportFilter filter,
			@Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom,
			@Param("wdwlRemCurrCode") Integer wdwlRemCurrCode, @Param("atmId") Integer atmId,
			@Param("nameAndAddr") String nameAndAddr);
	
	@Results({
			@Result(column = "ATM_ID", property = "atmID", javaType = Integer.class),
			@Result(column = "CURR_CODE", property = "currCode", javaType = Integer.class),
			@Result(column = "STAT_DATE", property = "statDate", javaType = Timestamp.class),
			@Result(column = "CODE_A3", property = "currCodeA3", javaType = String.class),
			@Result(column = "CURR_SUMM_IN", property = "currSummIn", javaType = Double.class, typeHandler = DoubleToLongHandler.class),
			@Result(column = "CURR_SUMM_OUT", property = "currSummOut", javaType = Double.class, typeHandler = DoubleToLongHandler.class),
			@Result(column = "CURR_REMAINING", property = "currRemaining", javaType = Double.class, typeHandler = DoubleToLongHandler.class),
			@Result(column = "ENCASHMENT_ID", property = "encashmentId", javaType = Integer.class)
	})
	@SelectProvider(type = ReportsWdwlRemBuilder.class, method = "getReportCrCurrWithdrawalRemainAtmBuilder")
	@ResultType(RepCurrWdwlRemRecItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepCurrWdwlRemRecItem> getReportCrCurrWithdrawalRemainAtm(@Param("filter") ReportFilter filter,
			@Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom,
			@Param("wdwlRemCurrCode") Integer wdwlRemCurrCode, @Param("atmId") Integer atmId,
			@Param("nameAndAddr") String nameAndAddr);

	@Results({
			@Result(column = "CURR_CODE", property = "currCode", javaType = Integer.class),
			@Result(column = "STAT_DATE", property = "statDate", javaType = Timestamp.class),
			@Result(column = "CODE_A3", property = "currCodeA3", javaType = String.class),
			@Result(column = "CURR_SUMM_IN", property = "currSummIn", javaType = Double.class, typeHandler = DoubleToLongHandler.class),
			@Result(column = "CURR_SUMM_OUT", property = "currSummOut", javaType = Double.class, typeHandler = DoubleToLongHandler.class),
			@Result(column = "CURR_REMAINING", property = "currRemaining", javaType = Double.class, typeHandler = DoubleToLongHandler.class)
	})
	@SelectProvider(type = ReportsWdwlRemBuilder.class, method = "getReportCrCurrWithdrawalRemainGroupBuilder")
	@ResultType(RepCurrWdwlRemRecItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepCurrWdwlRemRecItem> getReportCrCurrWithdrawalRemainGroup(@Param("filter") ReportFilter filter,
			@Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom,
			@Param("wdwlRemCurrCode") Integer wdwlRemCurrCode, @Param("atmId") Integer atmId,
			@Param("nameAndAddr") String nameAndAddr);

	@Results({
			@Result(column = "ATM_ID", property = "atmID", javaType = Integer.class),
			@Result(column = "STAT_DATE", property = "statDate", javaType = Timestamp.class),
			@Result(column = "CURR_SUMM", property = "currSumm", javaType = Double.class, typeHandler = DoubleToLongHandler.class),
			@Result(column = "CURR_REMAINING", property = "currRemaining", javaType = Double.class, typeHandler = DoubleToLongHandler.class),
			@Result(column = "ENCASHMENT_ID", property = "encashmentId", javaType = Integer.class),
	})
	@SelectProvider(type = ReportsWdwlRemBuilder.class, method = "getReportCiBillWithdrawalRemainAtmBuilder")
	@ResultType(RepCurrWdwlRemItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepCurrWdwlRemItem> getReportCiBillWithdrawalRemainAtm(@Param("filter") ReportFilter filter,
			@Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom, @Param("atmId") Integer atmId,
			@Param("nameAndAddr") String nameAndAddr);
	
	@Results({
			@Result(column = "STAT_DATE", property = "statDate", javaType = Timestamp.class),
			@Result(column = "CURR_SUMM", property = "currSumm", javaType = Double.class, typeHandler = DoubleToLongHandler.class),
			@Result(column = "CURR_REMAINING", property = "currRemaining", javaType = Double.class, typeHandler = DoubleToLongHandler.class)
	})
	@SelectProvider(type = ReportsWdwlRemBuilder.class, method = "getReportCiBillWithdrawalRemainGroupBuilder")
	@ResultType(RepCurrWdwlRemItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepCurrWdwlRemItem> getReportCiBillWithdrawalRemainGroup(@Param("filter") ReportFilter filter,
			@Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom, @Param("atmId") Integer atmId,
			@Param("nameAndAddr") String nameAndAddr);
	
	@Results({
			@Result(column = "ATM_ID", property = "atmID", javaType = Integer.class),
			@Result(column = "DENOM_CURR", property = "currCode", javaType = Integer.class),
			@Result(column = "STAT_DATE", property = "statDate", javaType = Timestamp.class),
			@Result(column = "CODE_A3", property = "currCodeA3", javaType = String.class),
			@Result(column = "CURR_SUMM", property = "currSumm", javaType = Double.class, typeHandler = DoubleToLongHandler.class),
			@Result(column = "CURR_REMAINING", property = "currRemaining", javaType = Double.class, typeHandler = DoubleToLongHandler.class),
			@Result(column = "ENCASHMENT_ID", property = "encashmentId", javaType = Integer.class),
	})
	@SelectProvider(type = ReportsWdwlRemBuilder.class, method = "getReportCiCurrWithdrawalRemainAtmBuilder")
	@ResultType(RepCurrWdwlRemItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepCurrWdwlRemItem> getReportCiCurrWithdrawalRemainAtm(@Param("filter") ReportFilter filter,
			@Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom,
			@Param("wdwlRemCurrCode") Integer wdwlRemCurrCode, @Param("atmId") Integer atmId,
			@Param("nameAndAddr") String nameAndAddr);
	
	@Results({
			@Result(column = "DENOM_CURR", property = "currCode", javaType = Integer.class),
			@Result(column = "STAT_DATE", property = "statDate", javaType = Timestamp.class),
			@Result(column = "CODE_A3", property = "currCodeA3", javaType = String.class),
			@Result(column = "CURR_SUMM", property = "currSumm", javaType = Double.class, typeHandler = DoubleToLongHandler.class),
			@Result(column = "CURR_REMAINING", property = "currRemaining", javaType = Double.class, typeHandler = DoubleToLongHandler.class)
	})
	@SelectProvider(type = ReportsWdwlRemBuilder.class, method = "getReportCiCurrWithdrawalRemainGroupBuilder")
	@ResultType(RepCurrWdwlRemItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepCurrWdwlRemItem> getReportCiCurrWithdrawalRemainGroup(@Param("filter") ReportFilter filter,
			@Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom,
			@Param("wdwlRemCurrCode") Integer wdwlRemCurrCode, @Param("atmId") Integer atmId,
			@Param("nameAndAddr") String nameAndAddr);

	@Results({
			@Result(column = "ATM_ID", property = "atmId", javaType = Integer.class),
			@Result(column = "DENOM_CURR", property = "denomCurr", javaType = Integer.class),
			@Result(column = "STAT_DATE", property = "statDate", javaType = Timestamp.class),
			@Result(column = "DENOM_VALUE", property = "denomValue", javaType = Integer.class),
			@Result(column = "DENOM_COUNT", property = "denomCount", javaType = Integer.class),
			@Result(column = "CODE_A3", property = "denomCurrA3", javaType = String.class),
			@Result(column = "ENCASHMENT_ID", property = "encashmentId", javaType = Integer.class),
			@Result(column = "DENOM_REMAINING", property = "denomRemaining", javaType = Integer.class)
	})
	@SelectProvider(type = ReportsWdwlRemBuilder.class, method = "getReportCoDenomWithdrawalRemainAtmBuilder")
	@ResultType(RepDenomWdwlRemItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepDenomWdwlRemItem> getReportCoDenomWithdrawalRemainAtm(@Param("filter") ReportFilter filter,
			@Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom,
			@Param("wdwlRemCurrCode") Integer wdwlRemCurrCode, @Param("atmId") Integer atmId,
			@Param("nameAndAddr") String nameAndAddr);
	
	@Results({
			@Result(column = "CURR_CODE", property = "denomCurr", javaType = Integer.class),
			@Result(column = "STAT_DATE", property = "statDate", javaType = Timestamp.class),
			@Result(column = "DENOM_VALUE", property = "denomValue", javaType = Integer.class),
			@Result(column = "CURR_SUMM", property = "denomCount", javaType = Integer.class),
			@Result(column = "CODE_A3", property = "denomCurrA3", javaType = String.class),
			@Result(column = "CURR_REMAINING", property = "denomRemaining", javaType = Integer.class)
	})
	@SelectProvider(type = ReportsWdwlRemBuilder.class, method = "getReportCoDenomWithdrawalRemainGroupBuilder")
	@ResultType(RepDenomWdwlRemItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepDenomWdwlRemItem> getReportCoDenomWithdrawalRemainGroup(@Param("filter") ReportFilter filter,
			@Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom,
			@Param("wdwlRemCurrCode") Integer wdwlRemCurrCode, @Param("atmId") Integer atmId,
			@Param("nameAndAddr") String nameAndAddr);
	
	@Results({
			@Result(column = "ATM_ID", property = "atmID", javaType = Integer.class),
			@Result(column = "DENOM_CURR", property = "denomCurr", javaType = Integer.class),
			@Result(column = "STAT_DATE", property = "statDate", javaType = Timestamp.class),
			@Result(column = "DENOM_VALUE", property = "denomValue", javaType = Integer.class),
			@Result(column = "DENOM_COUNT_IN", property = "denomCountIn", javaType = Integer.class),
			@Result(column = "DENOM_COUNT_OUT", property = "denomCountOut", javaType = Integer.class),
			@Result(column = "CODE_A3", property = "denomCurrA3", javaType = String.class),
			@Result(column = "DENOM_REMAINING", property = "denomRemaining", javaType = Integer.class),
			@Result(column = "ENCASHMENT_ID", property = "encashmentId", javaType = Integer.class)
	})
	@SelectProvider(type = ReportsWdwlRemBuilder.class, method = "getReportCrDenomWithdrawalRemainAtmBuilder")
	@ResultType(RepDenomWdwlRemRecItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepDenomWdwlRemRecItem> getReportCrDenomWithdrawalRemainAtm(@Param("filter") ReportFilter filter,
			@Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom,
			@Param("wdwlRemCurrCode") Integer wdwlRemCurrCode, @Param("atmId") Integer atmId,
			@Param("nameAndAddr") String nameAndAddr);
	
	@Results({
			@Result(column = "CURR_CODE", property = "denomCurr", javaType = Integer.class),
			@Result(column = "STAT_DATE", property = "statDate", javaType = Timestamp.class),
			@Result(column = "DENOM_VALUE", property = "denomValue", javaType = Integer.class),
			@Result(column = "CURR_SUMM_IN", property = "denomCountIn", javaType = Integer.class),
			@Result(column = "CURR_SUMM_IN", property = "denomCountOut", javaType = Integer.class),
			@Result(column = "CODE_A3", property = "denomCurrA3", javaType = String.class),
			@Result(column = "CURR_REMAINING", property = "denomRemaining", javaType = Integer.class)
	})
	@SelectProvider(type = ReportsWdwlRemBuilder.class, method = "getReportCrDenomWithdrawalRemainGroupBuilder")
	@ResultType(RepDenomWdwlRemRecItem.class)
	@Options(useCache = true, fetchSize = 1000)
	List<RepDenomWdwlRemRecItem> getReportCrDenomWithdrawalRemainGroup(@Param("filter") ReportFilter filter,
			@Param("dateTo") Timestamp dateTo, @Param("dateFrom") Timestamp dateFrom,
			@Param("wdwlRemCurrCode") Integer wdwlRemCurrCode, @Param("atmId") Integer atmId,
			@Param("nameAndAddr") String nameAndAddr);
}
