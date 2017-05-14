package ru.bpc.cm.optimization;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.cashmanagement.CmCommonController;
import ru.bpc.cm.cashmanagement.CurrencyConverter;
import ru.bpc.cm.constants.CashManagementConstants;
import ru.bpc.cm.forecasting.controllers.ForecastCommonController;
import ru.bpc.cm.forecasting.controllers.ForecastCompareController;
import ru.bpc.cm.forecasting.controllers.ForecastForPeriodController;
import ru.bpc.cm.items.enums.AtmAttribute;
import ru.bpc.cm.items.forecast.ForecastCurrencyItem;
import ru.bpc.cm.items.forecast.ForecastException;
import ru.bpc.cm.items.optimization.ForecastCompareAllGroupSummaryItem;
import ru.bpc.cm.items.optimization.ForecastCompareAtmSummaryItem;
import ru.bpc.cm.items.optimization.ForecastCompareCurrStat;
import ru.bpc.cm.items.optimization.ForecastCompareGroupSummaryItem;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.IFilterItem;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.Pair;

public class ForecastStatsCompareManager {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	public static ForecastCompareAllGroupSummaryItem getAllGroupForecastCompare(ISessionHolder sessionHolder,
			Connection connection, Date startDate, Date endDate,
			Map<IFilterItem<Integer>, List<IFilterItem<Integer>>> atmsToGroups, List<IFilterItem<Integer>> allAtms) {

		ForecastCompareAllGroupSummaryItem allGroupsItem = new ForecastCompareAllGroupSummaryItem();

		List<Integer> fullAtmList = new ArrayList<Integer>();
		for (IFilterItem<Integer> i : allAtms) {
			fullAtmList.add(i.getValue());
		}

		allGroupsItem.setId("All Groups");

		for (Entry<IFilterItem<Integer>, List<IFilterItem<Integer>>> s : atmsToGroups.entrySet()) {
			ForecastCompareGroupSummaryItem item = getOneGroupForecastCompare(sessionHolder, connection, startDate,
					endDate, s.getKey().getValue(), s.getValue());
			item.setupGroupParameters();
			allGroupsItem.getGroups().add(item);
		}

		allGroupsItem.setupGroupParameters();

		return allGroupsItem;
	}

	private static ForecastCompareGroupSummaryItem getOneGroupForecastCompare(ISessionHolder sessionHolder,
			Connection connection, Date startDate, Date endDate, Integer groupId, List<IFilterItem<Integer>> atms) {

		ForecastCompareGroupSummaryItem groupItem = new ForecastCompareGroupSummaryItem();

		try {
			Pair groupNameAndDescx = CmCommonController.getAtmGroupNameAndDescx(sessionHolder, groupId);
			groupItem.setId(groupNameAndDescx.getKey());
			groupItem.setDescx(groupNameAndDescx.getLabel());

		} catch (SQLException e) {

		}

		for (IFilterItem<Integer> atm : atms) {
			try {
				groupItem.getAtms().add(getForecastCompareSummary(sessionHolder, connection, atm.getValue(),
						atm.getLabel(), startDate, endDate));
			} catch (ForecastException e) {
				logger.error(String.valueOf(atm), e);
			}
		}

		return groupItem;
	}

	protected static ForecastCompareAtmSummaryItem getForecastCompareSummary(ISessionHolder sessionHolder,
			Connection connection, int atmId, String extAtmId, Date startDate, Date endDate) throws ForecastException {
		ForecastCompareAtmSummaryItem item = new ForecastCompareAtmSummaryItem();
		Map<Double, String> encCostPercents = null;
		List<ForecastCurrencyItem> currList = null;
		String atmInstId = null;
		int encLostsCurrCode = 0;
		int attrCostCurrCode = 0;

		item.setId(String.valueOf(atmId));
		item.setExtId(extAtmId);

		Pair atmAddressAndName = CmCommonController.getAtmAddressAndName(sessionHolder, atmId);

		item.setName(atmAddressAndName.getLabel());
		item.setDescx(atmAddressAndName.getKey());

		try {
			currList = ForecastCommonController.getAtmCurrencies(sessionHolder, atmId);
			atmInstId = CmCommonController.getInstIdForAtm(sessionHolder, atmId);
			encLostsCurrCode = Integer
					.parseInt(CmCommonController.getAtmAttribute(sessionHolder, atmId, AtmAttribute.REGION_CURR_CODE));
			attrCostCurrCode = Integer.parseInt(
					CmCommonController.getAtmAttribute(sessionHolder, atmId, AtmAttribute.ENC_COST_CURR_CODE));
			encCostPercents = ForecastCommonController.getEncCostPercents(sessionHolder, atmId,
					new CurrencyConverter(sessionHolder, atmId, encLostsCurrCode, attrCostCurrCode),
					AtmAttribute.ENC_COST_LEVELS_SUMMS, attrCostCurrCode, encLostsCurrCode);

		} catch (ForecastException e) {
			item.setForecastingWasPerformed(false);
			item.setEnoughStatistics(false);
			return item;
		}

		int periodLength = CmUtils.getDaysBetweenTwoDates(startDate, endDate);

		if (periodLength > 0) {
			item.setForecastingWasPerformed(
					ForecastStatsCompareController.checkForecastPerformedForAtm(sessionHolder, atmId, startDate, endDate));
			item.setEnoughStatistics(checkEnoughStatistics(sessionHolder, atmId));
			item.setEncCountForecast(CompareForecastController.getEncCount(sessionHolder, atmId, startDate, endDate));
			item.setEncCountStats(CompareStatsController.getEncCount(sessionHolder, atmId, startDate, endDate));
			item.setEncLostsCurr(encLostsCurrCode);
			item.setEncLostsCurrA3(CmCommonController.getCurrCodeA3(sessionHolder, encLostsCurrCode));
			item.setEncLostsForecast(Math.round(getEncLostsForecast(sessionHolder, connection, atmId, encLostsCurrCode,
					currList, startDate, endDate, atmInstId)));
			item.setEncLostsStats(Math.round(getEncLostsStats(sessionHolder, connection, atmId, encLostsCurrCode,
					currList, startDate, endDate, atmInstId)));
			item.setEncPriceForecast(Math.round(getEncPriceForecast(sessionHolder, connection, encLostsCurrCode, atmId,
					startDate, endDate, atmInstId)));
			item.setEncPriceStats(Math.round(getEncPriceStats(sessionHolder, connection, encLostsCurrCode,
					attrCostCurrCode, atmId, encCostPercents, startDate, endDate, atmInstId)));
			item.setAtmLostsForecast(item.getEncLostsForecast() + item.getEncPriceForecast());
			item.setAtmLostsStats(item.getEncLostsStats() + item.getEncPriceStats());
			item.setAtmSaves((item.getAtmLostsStats() - item.getAtmLostsForecast()) * 30 / periodLength);
		}

		return item;
	}

	private static boolean checkEnoughStatistics(ISessionHolder sessionHolder, int atmId) {
		Date endDate = ForecastForPeriodController.getStatsEnd(sessionHolder, atmId, Calendar.getInstance().getTime());

		Calendar startDateCal = Calendar.getInstance();
		startDateCal.setTime(endDate);
		startDateCal.add(Calendar.MONTH, -CashManagementConstants.COMPARE_STATS_ENOUGH_PERIOD_MONTH);

		return (ForecastCompareController.getStatsDatesCount(sessionHolder, atmId, startDateCal.getTime(),
				endDate) >= 0);

	}

	public static Map<Integer, List<ForecastCompareCurrStat>> getCurrRemainings(ISessionHolder sessionHolder, int atmId,
			List<Integer> currencies, Date startDate, Date endDate) {
		Map<Integer, List<ForecastCompareCurrStat>> currStatMap = new LinkedHashMap<Integer, List<ForecastCompareCurrStat>>();
		for (Integer curr : currencies) {
			currStatMap.put(curr,
					ForecastStatsCompareController.getCurrRemainings(sessionHolder, atmId, curr, startDate, endDate));
		}
		return currStatMap;
	}

	private static double getEncLostsStats(ISessionHolder sessionHolder, Connection connection, int atmId,
			int encLostsCurrCode, List<ForecastCurrencyItem> currList, Date startDate, Date endDate, String atmInstId) {
		double encLosts = 0;
		double encLostStatsForCurr = 0;
		for (ForecastCurrencyItem curr : currList) {
			encLostStatsForCurr = CompareStatsController.getEncLostsForCurr(sessionHolder, atmId, curr.getCurrCode(),
					startDate, endDate)
					+ CompareStatsController.getPeriodStartLostsForCurr(sessionHolder, atmId, curr.getCurrCode(),
							startDate)
					+ CompareStatsController.getPeriodEndLostsForCurr(sessionHolder, atmId, curr.getCurrCode(), endDate);

			encLosts += curr.getRefinancingRate() * CmCommonController.convertValue(sessionHolder, curr.getCurrCode(),
					encLostsCurrCode, encLostStatsForCurr, atmInstId) / 1752000;
		}
		return encLosts;
	}

	private static double getEncLostsForecast(ISessionHolder sessionHolder, Connection connection, int atmId,
			int encLostsCurrCode, List<ForecastCurrencyItem> currList, Date startDate, Date endDate, String atmInstId) {
		double encLosts = 0;
		double encLostForCurr = 0;

		for (ForecastCurrencyItem curr : currList) {
			encLostForCurr = CompareForecastController.getEncLostsForCurr(sessionHolder, atmId, curr.getCurrCode(),
					startDate, endDate);
			encLosts += curr.getRefinancingRate() * CmCommonController.convertValue(sessionHolder, curr.getCurrCode(),
					encLostsCurrCode, encLostForCurr, atmInstId) / 1752000;
		}
		ObjectPair<Integer, Long> lastEncLosts = CompareForecastController.getEncLostsForLastEnc(sessionHolder, atmId,
				startDate, endDate);

		encLosts += CmCommonController.convertValue(sessionHolder, lastEncLosts.getKey(), encLostsCurrCode,
				lastEncLosts.getValue().doubleValue(), atmInstId) / 1752000;
		return encLosts;
	}

	private static double getEncPriceStats(ISessionHolder sessionHolder, Connection connection, int encLostsCurrCode,
			int attrCostCurrCode, int atmId, Map<Double, String> encCostPercents, Date startDate, Date endDate,
			String atmInstId) throws ForecastException {
		double encPrice = 0;
		List<List<ObjectPair<Integer, Long>>> encStatCurrList = null;
		double encCoCostFix = Double
				.parseDouble(CmCommonController.getAtmAttribute(sessionHolder, atmId, AtmAttribute.ENC_COST_CASH_OUT));
		double encCiCostFix = Double
				.parseDouble(CmCommonController.getAtmAttribute(sessionHolder, atmId, AtmAttribute.ENC_COST_CASH_IN));
		double encJointCostFix = Double.parseDouble(
				CmCommonController.getAtmAttribute(sessionHolder, atmId, AtmAttribute.ENC_COST_BOTH_IN_OUT));

		encCoCostFix = CmCommonController
				.convertValue(sessionHolder, attrCostCurrCode, encLostsCurrCode, encCoCostFix, atmInstId).doubleValue();
		encCiCostFix = CmCommonController
				.convertValue(sessionHolder, attrCostCurrCode, encLostsCurrCode, encCiCostFix, atmInstId).doubleValue();
		encJointCostFix = CmCommonController
				.convertValue(sessionHolder, attrCostCurrCode, encLostsCurrCode, encJointCostFix, atmInstId)
				.doubleValue();

		encStatCurrList = CompareStatsController.getSplitEncCurrList(sessionHolder, atmId, startDate, endDate);

		for (List<ObjectPair<Integer, Long>> list : encStatCurrList) {
			double encSum = 0;
			double encCostPercent = 0;

			for (ObjectPair<Integer, Long> pair : list) {
				encSum += CmCommonController.convertValue(sessionHolder, pair.getKey(), encLostsCurrCode,
						pair.getValue().doubleValue(), atmInstId);
			}
			encCostPercent = Double.parseDouble(getEncCostPercent(encSum, encCostPercents));
			encPrice += (encCoCostFix + encSum * encCostPercent / 100);
		}

		encStatCurrList = CompareStatsController.getJointEncCurrList(sessionHolder, atmId, startDate, endDate);

		for (List<ObjectPair<Integer, Long>> list : encStatCurrList) {
			double encSum = 0;
			double encCostPercent = 0;

			for (ObjectPair<Integer, Long> pair : list) {
				encSum += CmCommonController.convertValue(sessionHolder, pair.getKey(), encLostsCurrCode,
						pair.getValue().doubleValue(), atmInstId);
			}
			encCostPercent = Double.parseDouble(getEncCostPercent(encSum, encCostPercents));
			encPrice += (encJointCostFix + encSum * encCostPercent / 100);
		}

		encPrice += encCiCostFix * CompareStatsController.getSplitCiEncCount(sessionHolder, atmId, startDate, endDate);

		return encPrice;
	}

	private static double getEncPriceForecast(ISessionHolder sessionHolder, Connection connection, int encLostsCurrCode,
			int atmId, Date startDate, Date endDate, String atmInstId) throws ForecastException {
		ObjectPair<Integer, Long> encForecastPrice = CompareForecastController.getEncPriceWithCurr(sessionHolder, atmId,
				startDate, endDate);
		return CmCommonController.convertValue(sessionHolder, encForecastPrice.getKey(), encLostsCurrCode,
				encForecastPrice.getValue().doubleValue(), atmInstId);

	}

	private static String getEncCostPercent(Double encSumm, Map<Double, String> encCostPercents) {
		String attributeValue = "0";
		if (encCostPercents != null) {
			for (Double summ : encCostPercents.keySet()) {
				if (encSumm > summ) {
					attributeValue = encCostPercents.get(summ);
				}
			}
		}
		return attributeValue;
	}

}
