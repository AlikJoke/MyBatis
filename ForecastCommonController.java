package ru.bpc.cm.forecasting.controllers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.cashmanagement.AtmCassettesController;
import ru.bpc.cm.cashmanagement.CurrencyConverter;
import ru.bpc.cm.forecasting.orm.ForecastCommonMapper;
import ru.bpc.cm.items.enums.AtmAttribute;
import ru.bpc.cm.items.enums.AtmCassetteType;
import ru.bpc.cm.items.enums.AtmGroupType;
import ru.bpc.cm.items.enums.CalendarDayType;
import ru.bpc.cm.items.enums.ForecastingMode;
import ru.bpc.cm.items.forecast.ForecastCurrencyItem;
import ru.bpc.cm.items.forecast.ForecastException;
import ru.bpc.cm.settings.AtmGroupController;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.ObjectPair;
import ru.bpc.cm.utils.db.JdbcUtils;

public class ForecastCommonController {

	static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<ForecastCommonMapper> getMapperClass() {
		return ForecastCommonMapper.class;
	}

	public static List<ForecastCurrencyItem> getAtmCurrencies(ISessionHolder sessionHolder, int atmId)
			throws ForecastException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<ForecastCurrencyItem> atmCurrencies = new ArrayList<ForecastCurrencyItem>();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int cassCountForSecCurr = 0;
		int cassCountForSec2Curr = 0;
		int cassCountForSec3Curr = 0;
		try {
			String query = "SELECT " + "MAIN_CURR_CODE, MAIN_CURR_RATE, " + "SECONDARY_CURR_CODE, SEC_CURR_RATE, "
					+ "SECONDARY2_CURR_CODE, SEC2_CURR_RATE, " + "SECONDARY3_CURR_CODE, SEC3_CURR_RATE "
					+ "FROM V_CM_ATM_CURR " + "WHERE ATM_ID = ?";
			pstmt = session.getConnection().prepareStatement(query);
			pstmt.setInt(1, atmId);
			rs = pstmt.executeQuery();
			if (rs.next()) {
				ForecastCurrencyItem item = new ForecastCurrencyItem();
				item.setCurrCode(rs.getInt("MAIN_CURR_CODE"));
				item.setRefinancingRate(rs.getDouble("MAIN_CURR_RATE"));
				atmCurrencies.add(item);
				if (rs.getInt("SECONDARY_CURR_CODE") > 0) {
					cassCountForSecCurr = 1;
					item = new ForecastCurrencyItem();
					item.setCurrCode(rs.getInt("SECONDARY_CURR_CODE"));
					item.setRefinancingRate(rs.getDouble("SEC_CURR_RATE"));
					item.setMaxCassCount(cassCountForSecCurr);
					atmCurrencies.add(item);
				}
				if (rs.getInt("SECONDARY2_CURR_CODE") > 0) {
					cassCountForSec2Curr = 1;
					item = new ForecastCurrencyItem();
					item.setCurrCode(rs.getInt("SECONDARY2_CURR_CODE"));
					item.setRefinancingRate(rs.getDouble("SEC2_CURR_RATE"));
					item.setMaxCassCount(cassCountForSec2Curr);
					atmCurrencies.add(item);
				}
				if (rs.getInt("SECONDARY3_CURR_CODE") > 0) {
					cassCountForSec3Curr = 1;
					item = new ForecastCurrencyItem();
					item.setCurrCode(rs.getInt("SECONDARY3_CURR_CODE"));
					item.setRefinancingRate(rs.getDouble("SEC3_CURR_RATE"));
					item.setMaxCassCount(cassCountForSec3Curr);
					atmCurrencies.add(item);
				}
				atmCurrencies.get(0).setMaxCassCount(AtmCassettesController.getAtmCassCount(sessionHolder,
						atmId, AtmCassetteType.CASH_OUT_CASS));
			} else {
				throw new ForecastException(ForecastException.NO_MAIN_CURR);
			}
		} catch (SQLException e) {
			logger.error("", e);
		} finally {
			JdbcUtils.close(pstmt);
			JdbcUtils.close(rs);
		}
		return atmCurrencies;
	}

	public static Map<Double, String> getEncCostPercents(ISessionHolder sessionHolder, int atmId,
			CurrencyConverter converter, AtmAttribute atmAttribute, int encCostCurr, int encRegionCurr) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Map<Double, String> tmpMap = new LinkedHashMap<Double, String>();
		Map<Double, String> encCostsPercents = new LinkedHashMap<Double, String>();
		Double attributeValue = null;
		try {
			List<ObjectPair<Double, String>> result = session.getMapper(getMapperClass())
					.getEncCostPercents(atmAttribute.getAttrID(), atmAttribute.getGroupType().getId(), atmId);

			int i = 0;
			for (ObjectPair<Double, String> pair : result) {
				attributeValue = pair.getKey();
				if (i < result.size() - 1)
					tmpMap.put(attributeValue, pair.getValue());
				i++;
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		for (Entry<Double, String> entry : tmpMap.entrySet())
			encCostsPercents.put(converter.convertValue(encCostCurr, encRegionCurr, entry.getKey()).doubleValue(),
					entry.getValue());

		return encCostsPercents;
	}

	public static Map<Date, Integer> getAvailableAtmDaysForecast(ISessionHolder sessionHolder, Connection con,
			int atmId, Date startDate, Date endDate, int techTime, boolean emergencyEncashment,
			boolean firstForecastForPeriod, ForecastingMode mode) {

		Map<Date, Integer> forecastDaysMap = new HashMap<Date, Integer>();
		Map<Date, Integer> encDaysMap = new HashMap<Date, Integer>();

		Calendar startDateCal = Calendar.getInstance();
		Calendar endDateCal = Calendar.getInstance();

		startDateCal.setTime(startDate);
		endDateCal.setTime(endDate);

		if (mode == ForecastingMode.PLAN || firstForecastForPeriod) {
			startDateCal.add(Calendar.HOUR_OF_DAY, techTime);
			if (emergencyEncashment) {
				endDateCal.add(Calendar.DAY_OF_YEAR, 7);
			}
		}

		for (Date day : getEncAvailableDaysForecast(sessionHolder, atmId, startDateCal.getTime(), endDateCal.getTime(),
				null)) {
			forecastDaysMap.put(day, 0);
		}

		int atmCountGroup = AtmGroupController.getAtmGroupIdByType(con, atmId, AtmGroupType.FORECAST_REGION);
		switch (mode) {
		case COMPARE:
			encDaysMap.putAll(
					getEncMapDaysCompare(sessionHolder, atmCountGroup, startDateCal.getTime(), endDateCal.getTime()));
			break;
		case PERIOD:
			encDaysMap.putAll(
					getEncMapDaysPeriod(sessionHolder, atmCountGroup, startDateCal.getTime(), endDateCal.getTime()));
			break;
		case PLAN:
			encDaysMap.putAll(
					getEncMapDaysPlan(sessionHolder, atmCountGroup, startDateCal.getTime(), endDateCal.getTime()));
			break;
		default:
			break;

		}

		for (Entry<Date, Integer> entry : forecastDaysMap.entrySet()) {
			entry.setValue(CmUtils.getNVLValue(encDaysMap.get(CmUtils.truncateDate(entry.getKey())), 0));
		}

		return forecastDaysMap;
	}

	private static Map<Date, Integer> getEncMapDaysPlan(ISessionHolder sessionHolder, int atmCountGroup, Date startDate,
			Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Map<Date, Integer> encDaysMap = new HashMap<Date, Integer>();

		try {
			List<ObjectPair<Timestamp, Integer>> result = session.getMapper(getMapperClass()).getEncMapDaysPlan(
					new Timestamp(startDate.getTime()), new Timestamp(endDate.getTime()), atmCountGroup);

			for (ObjectPair<Timestamp, Integer> pair : result)
				encDaysMap.put(pair.getKey(), pair.getValue());
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return encDaysMap;
	}

	private static Map<Date, Integer> getEncMapDaysPeriod(ISessionHolder sessionHolder, int atmCountGroup,
			Date startDate, Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Map<Date, Integer> encDaysMap = new HashMap<Date, Integer>();

		try {
			List<ObjectPair<Timestamp, Integer>> result = session.getMapper(getMapperClass()).getEncMapDaysPeriod(
					new Timestamp(startDate.getTime()), new Timestamp(endDate.getTime()), atmCountGroup);

			for (ObjectPair<Timestamp, Integer> pair : result)
				encDaysMap.put(pair.getKey(), pair.getValue());

		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return encDaysMap;
	}

	private static Map<Date, Integer> getEncMapDaysCompare(ISessionHolder sessionHolder, int atmCountGroup,
			Date startDate, Date endDate) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Map<Date, Integer> encDaysMap = new HashMap<Date, Integer>();

		try {
			List<ObjectPair<Timestamp, Integer>> result = session.getMapper(getMapperClass()).getEncMapDaysCompare(
					new Timestamp(startDate.getTime()), new Timestamp(endDate.getTime()), atmCountGroup);

			for (ObjectPair<Timestamp, Integer> pair : result)
				encDaysMap.put(pair.getKey(), pair.getValue());

		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return encDaysMap;
	}

	public static Set<Date> getAtmAvailableDaysForecast(ISessionHolder sessionHolder, int atmId, int period,
			Date startDate, Set<Date> dates) {

		if (dates == null) {
			dates = new HashSet<Date>();
		}

		Calendar finishCal = Calendar.getInstance();
		finishCal.setTime(startDate);
		finishCal.add(Calendar.HOUR_OF_DAY, period);
		Date finishDate = finishCal.getTime();
		dates.addAll(CmUtils.getHourlyDatesBetweenTwoDates(startDate, finishCal.getTime()));
		dates.removeAll(getAtmNotAvailableDates(sessionHolder, finishCal.getTime(), startDate, atmId));

		if (dates.size() < period) {
			return getAtmAvailableDaysForecast(sessionHolder, atmId, period - dates.size(), finishDate, dates);
		} else {
			return dates;
		}
	}

	private static Set<Date> getEncAvailableDaysForecast(ISessionHolder sessionHolder, int atmId, Date startDate,
			Date finishDate, Set<Date> dates) {

		if (dates == null) {
			dates = new HashSet<Date>();
		}

		dates.addAll(CmUtils.getHourlyDatesBetweenTwoDates(startDate, finishDate));
		dates.removeAll(getEncNotAvailableDates(sessionHolder, finishDate, startDate, atmId));

		return dates;
	}

	public static int getPeriodAvailableDates(ISessionHolder sessionHolder, Date finish, Date start, int atmId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		int periodAvailableDates = 0;
		try {
			periodAvailableDates = CmUtils.getHoursBetweenTwoDates(start, finish);

			Integer days = session.getMapper(getMapperClass()).getPeriodAvailableDates(
					CalendarDayType.ATM_NOT_AVAILABLE.getId(), atmId, new Timestamp(start.getTime()),
					new Timestamp(finish.getTime()));
			if (days != null)
				periodAvailableDates -= days;
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return periodAvailableDates;
	}

	private static Set<Date> getAtmNotAvailableDates(ISessionHolder sessionHolder, Date finish, Date start, int atmId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Set<Date> notAvailableDates = new HashSet<Date>();
		try {
			List<Timestamp> dates = session.getMapper(getMapperClass()).getAtmNotAvailableDates(atmId,
					CalendarDayType.ATM_NOT_AVAILABLE.getCalendarDayType(), new Timestamp(start.getTime()),
					new Timestamp(finish.getTime()));

			for (Timestamp date : dates)
				notAvailableDates.add(CmUtils.truncateDateToHours(date));

		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return notAvailableDates;
	}

	private static Set<Date> getEncNotAvailableDates(ISessionHolder sessionHolder, Date finish, Date start, int atmId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Set<Date> notAvailableDates = new HashSet<Date>();
		try {
			List<Timestamp> dates = session.getMapper(getMapperClass()).getEncNotAvailableDates(atmId,
					new Timestamp(start.getTime()), new Timestamp(finish.getTime()));

			for (Timestamp date : dates)
				notAvailableDates.add(CmUtils.truncateDateToHours(date));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return notAvailableDates;
	}

}
