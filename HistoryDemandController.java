package ru.bpc.cm.forecasting.controllers;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.forecasting.orm.HistoryDemandMapper;
import ru.bpc.cm.items.enums.CurrencyMode;
import ru.bpc.cm.items.monitoring.DemandCompareFilter;
import ru.bpc.cm.items.monitoring.DemandCompareItem;
import ru.bpc.cm.items.monitoring.HourDemandCompareItem;
import ru.bpc.cm.items.settings.calendars.coeffs.HistoryDemandItem;
import ru.bpc.cm.utils.db.JdbcUtils;

public class HistoryDemandController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");

	private static Class<HistoryDemandMapper> getMapperClass() {
		return HistoryDemandMapper.class;
	}

	public static List<DemandCompareItem> getDemandCompareForAtm(ISessionHolder sessionHolder,
			DemandCompareFilter filter) {
		List<DemandCompareItem> calendarDaysList = new ArrayList<DemandCompareItem>();

		CurrencyMode mode = filter.getCurrencyModeAsEnum();
		if (mode == null) {
			return calendarDaysList;
		}

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			calendarDaysList.addAll(session.getMapper(getMapperClass()).getDemandCompareForAtm(mode.getStatViewName(),
					filter.getAtmId(), JdbcUtils.getSqlDate(filter.getDateFrom()),
					JdbcUtils.getSqlDate(filter.getDateTo()), filter.getCurrency(), mode.getId()));

			for (DemandCompareItem item : calendarDaysList) {
				item.setCurrency(filter.getCurrency());
				item.setCurrencyMode(mode);
			}

		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}

		return calendarDaysList;
	}

	public static List<HourDemandCompareItem> getDemandHourCompareForAtm(ISessionHolder sessionHolder,
			DemandCompareFilter filter, Date day) {
		List<HourDemandCompareItem> calendarDayHoursList = new ArrayList<HourDemandCompareItem>();

		CurrencyMode mode = filter.getCurrencyModeAsEnum();
		if (mode == null) {
			return calendarDayHoursList;
		}

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			calendarDayHoursList
					.addAll(session.getMapper(getMapperClass()).getDemandHourCompareForAtm(mode.getStatViewName(),
							filter.getAtmId(), JdbcUtils.getSqlDate(day), filter.getCurrency(), mode.getId()));
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}

		return calendarDayHoursList;
	}

	public static void saveAtmCalendarDays(ISessionHolder sessionHolder, int atmId, Date statsEndDate,
			List<HistoryDemandItem> calendarDaysList) {
		SqlSession session = sessionHolder.getSession(getMapperClass(), ExecutorType.BATCH);
		try {
			HistoryDemandMapper mapper = session.getMapper(getMapperClass());
			mapper.deleteAtmCalendarDays(atmId, JdbcUtils.getSqlDate(statsEndDate));
			mapper.flush();

			for (HistoryDemandItem day : calendarDaysList)
				mapper.saveAtmCalendarDays(day.getAtmId(), day.getCurrency(), JdbcUtils.getSqlTimestamp(day.getDay()),
						day.getCurrencyMode().getId(), day.getDemand());
			if (!calendarDaysList.isEmpty())
				mapper.flush();
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static void changeAtmCalendarDay(ISessionHolder sessionHolder, DemandCompareItem day, boolean disable) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Calendar cal = Calendar.getInstance();
			cal.setTime(day.getDay());
			cal.add(Calendar.DAY_OF_YEAR, 1);

			session.getMapper(getMapperClass()).changeAtmCalendarDay(disable ? 1 : 0, day.getAtmId(),
					JdbcUtils.getSqlDate(day.getDay()), JdbcUtils.getSqlDate(cal.getTime()), day.getCurrency(),
					day.getCurrencyMode().getId());
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

	public static void changeAtmCalendarHour(ISessionHolder sessionHolder, DemandCompareItem day, int hour,
			boolean disable) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			Calendar cal = Calendar.getInstance();
			cal.setTime(day.getDay());
			cal.add(Calendar.HOUR_OF_DAY, hour);

			session.getMapper(getMapperClass()).changeAtmCalendarHour(disable ? 1 : 0, day.getAtmId(),
					JdbcUtils.getSqlDate(cal.getTime()), day.getCurrency(), day.getCurrencyMode().getId());
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
	}

}
