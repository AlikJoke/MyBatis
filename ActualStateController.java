package ru.bpc.cm.monitoring;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ejbs.cm.svcm.ISessionHolder;
import ru.bpc.cm.cashmanagement.CurrencyConverter;
import ru.bpc.cm.cashmanagement.orm.handlers.AtmCassetteInResultHandler;
import ru.bpc.cm.cashmanagement.orm.handlers.AtmCassetteOutResultHandler;
import ru.bpc.cm.cashmanagement.orm.items.BalanceItem;
import ru.bpc.cm.cashmanagement.orm.items.CodesItem;
import ru.bpc.cm.cashmanagement.orm.items.ObjectWrapper;
import ru.bpc.cm.config.utils.ORMUtils;
import ru.bpc.cm.filters.MonitoringFilter;
import ru.bpc.cm.forecasting.anyatm.items.AnyAtmForecast;
import ru.bpc.cm.forecasting.anyatm.items.Currency;
import ru.bpc.cm.items.enums.AtmCassetteType;
import ru.bpc.cm.items.forecast.ForecastStatDay;
import ru.bpc.cm.items.monitoring.AtmActualStateItem;
import ru.bpc.cm.items.monitoring.AtmCashOutCassetteItem;
import ru.bpc.cm.items.monitoring.AtmCassetteItem;
import ru.bpc.cm.items.monitoring.AtmRecyclingCassetteItem;
import ru.bpc.cm.items.monitoring.MonitoringException;
import ru.bpc.cm.monitoring.orm.AtmActualStateMapper;
import ru.bpc.cm.utils.CmUtils;
import ru.bpc.cm.utils.ObjectPair;

public class ActualStateController {

	private static final Logger logger = LoggerFactory.getLogger("CASH_MANAGEMENT");
	private static final double ALERT_DELTA_LOW = 30000;
	private static final double ALERT_DELTA_HIGH = 300000;
	private static final int ALERT_DELTA_CURR = 810;
	private static final int BALANCES_CHECK_THRESHHOLD = 200;
	private static final String BALANCES_CHECK_ERROR_FORMAT = "Wrong balance for ATM: ID={}; CASS_TYPE = {}; CASS_NUMBER = {}; LOADED_REMAINING = {}; CALCULATED_REMAINING = {}";

	private static Class<AtmActualStateMapper> getMapperClass() {
		return AtmActualStateMapper.class;
	}

	public static List<AtmActualStateItem> getAtmActualStateList(ISessionHolder sessionHolder,
			MonitoringFilter addFilter) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmActualStateMapper mapper = session.getMapper(getMapperClass());
			List<AtmActualStateItem> atmActualStateList = Optional.ofNullable(mapper.getAtmActualStateList(addFilter))
					.orElse(new ArrayList<AtmActualStateItem>());

			for (AtmActualStateItem item : atmActualStateList) {
				List<AtmCashOutCassetteItem> atmCashOutCassettes = Optional
						.ofNullable(mapper.getAtmCashOutCassettesList(item.getAtmID(),
								AtmCassetteType.CASH_OUT_CASS.getId(), item.getEncID(),
								new Timestamp(item.getStatDate().getTime())))
						.orElse(new ArrayList<AtmCashOutCassetteItem>());
				item.getCashOutCassettes().addAll(atmCashOutCassettes);
				List<AtmRecyclingCassetteItem> atmRecyclingCassette = Optional
						.ofNullable(mapper.getAtmRecyclingCassettesList(item.getAtmID(), item.getCashInEncId(),
								new Timestamp(item.getStatDate().getTime()), AtmCassetteType.CASH_IN_R_CASS.getId()))
						.orElse(new ArrayList<AtmRecyclingCassetteItem>());
				item.getCashInRCassettes().addAll(atmRecyclingCassette);
			}
			return atmActualStateList;
		} finally {
			session.close();
		}
	}

	private static int checkDeltaCoeff(int lastHourDemand, int lastThreeHourDemand, double deltaLow, double deltaHigh) {
		if (lastThreeHourDemand == 0) {
			if (lastHourDemand > deltaLow) {
				return lastHourDemand;
			}
		} else {
			double coeff = (double) lastHourDemand / (double) lastThreeHourDemand;

			if (coeff >= 0.35 && coeff <= 3.0) {
				if (Math.abs(lastThreeHourDemand - lastHourDemand) > deltaHigh) {
					return Math.abs(lastThreeHourDemand - lastHourDemand);
				}
			} else {
				if (Math.abs(lastThreeHourDemand - lastHourDemand) > deltaLow) {
					return Math.abs(lastThreeHourDemand - lastHourDemand);
				}
			}
		}
		return 0;
	}

	protected static List<Integer> getCurrenciesList(ISessionHolder sessionHolder, int atmId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		List<Integer> res = new ArrayList<Integer>();
		try {
			for (CodesItem item : session.getMapper(getMapperClass()).getCurrenciesList(atmId)) {
				res.add(item.getMainCurrCode());
				res.add(item.getSecCurrCode());
				res.add(item.getSec2CurrCode());
				res.add(item.getSec3CurrCode());
			}
		} catch (Exception e) {
			logger.error("atmID = " + atmId, e);
		} finally {
			session.close();
		}
		return res;
	}

	public static ObjectPair<Date, Integer> getCashOutLastStat(ISessionHolder sessionHolder, int atmId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		ObjectPair<Date, Integer> res = null;
		try {
			res = session.getMapper(getMapperClass()).getCashOutLastStat(atmId);
		} finally {
			session.close();
		}
		return res;
	}

	protected static int getCashOutHoursFromLastWithdrawal(ISessionHolder sessionHolder, int atmId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		int res = 0;
		try {
			ObjectWrapper<Integer> wrapper = session.getMapper(getMapperClass())
					.getCashOutHoursFromLastWithdrawal(atmId, ORMUtils.getSingleRowExpression(session));
			if (wrapper.getObject() != null)
				res = wrapper.getObject();
		} finally {
			session.close();
		}
		return res;
	}

	protected static int getCashInHoursFromLastAddition(ISessionHolder sessionHolder, int atmId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		int res = 0;
		try {
			ObjectWrapper<Integer> wrapper = session.getMapper(getMapperClass()).getCashInHoursFromLastAddition(atmId,
					ORMUtils.getSingleRowExpression(session));
			if (wrapper.getObject() != null)
				res = wrapper.getObject();
		} finally {
			session.close();
		}
		return res;
	}

	public static ObjectPair<Date, Integer> getCashInLastStat(ISessionHolder sessionHolder, int atmId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		ObjectPair<Date, Integer> res = null;
		try {
			res = session.getMapper(getMapperClass()).getCashInLastStat(atmId);
		} finally {
			session.close();
		}
		return res;
	}

	protected static double getAverageDemandStat(Map<Date, ForecastStatDay> days, Date startDate, Date endDate,
			int atmNotAvailableDays) {
		Date currentDate;
		double averageDemand = 0;
		double takeOffSumm = 0;
		int daysSize = 7;
		// daysSize = days.size() == 0 ? 1 : days.size();
		for (Entry<Date, ForecastStatDay> entry : days.entrySet()) {
			currentDate = CmUtils.truncateDate(entry.getKey());
			if ((currentDate.compareTo(startDate) == 0 || currentDate.after(startDate))
					&& (currentDate.compareTo(endDate) == 0 || currentDate.before(endDate)))
				takeOffSumm += entry.getValue().getTakeOffs();
		}

		averageDemand = takeOffSumm / (daysSize - atmNotAvailableDays);

		return (averageDemand / (daysSize - atmNotAvailableDays));
	}

	// UPDATE LAST_UPDATE_DATE ONLY ON TASK CALL
	protected static void actualizeAtmStateForAtm(ISessionHolder sessionHolder, AnyAtmForecast forecast,
			ObjectPair<Date, Integer> cashOutStat, ObjectPair<Date, Integer> cashInStat, boolean updateLastUpdateDate) {
		int check = 0;

		List<Currency> currencies = forecast.getAtmCurrencies();
		int currCount = currencies == null ? 0 : currencies.size();

		int mainCurrInDifference = 0;
		int mainCurrOutDifference = 0;
		int secCurrInDifference = 0;
		int secCurrOutDifference = 0;
		int sec2CurrInDifference = 0;
		int sec2CurrOutDifference = 0;
		int sec3CurrInDifference = 0;
		int sec3CurrOutDifference = 0;

		Currency mainCurrItem = currCount > 0 ? currencies.get(0) : null;
		Currency secCurrItem = currCount > 1 ? currencies.get(1) : null;
		Currency sec2CurrItem = currCount > 2 ? currencies.get(2) : null;
		Currency sec3CurrItem = currCount > 3 ? currencies.get(3) : null;
		CurrencyConverter converter = forecast.getCurrencyConverter();

		if (mainCurrItem != null) {
			mainCurrInDifference = checkDeltaCoeff(mainCurrItem.getAvgStatRecInCurrLastHourDemand(),
					mainCurrItem.getAvgStatRecInCurrLastThreeHoursDemand(),
					converter.convertValue(ALERT_DELTA_CURR, mainCurrItem.getCurrCode(), ALERT_DELTA_LOW),
					converter.convertValue(ALERT_DELTA_CURR, mainCurrItem.getCurrCode(), ALERT_DELTA_HIGH));
			mainCurrOutDifference = checkDeltaCoeff(
					mainCurrItem.getAvgStatOutLastHourDemand() + mainCurrItem.getAvgStatRecOutCurrLastHourDemand(),
					mainCurrItem.getAvgStatOutLastThreeHoursDemand()
							+ mainCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand(),
					converter.convertValue(ALERT_DELTA_CURR, mainCurrItem.getCurrCode(), ALERT_DELTA_LOW),
					converter.convertValue(ALERT_DELTA_CURR, mainCurrItem.getCurrCode(), ALERT_DELTA_HIGH));
		}
		if (secCurrItem != null) {
			secCurrInDifference = checkDeltaCoeff(secCurrItem.getAvgStatRecInCurrLastHourDemand(),
					secCurrItem.getAvgStatRecInCurrLastThreeHoursDemand(),
					converter.convertValue(ALERT_DELTA_CURR, secCurrItem.getCurrCode(), ALERT_DELTA_LOW),
					converter.convertValue(ALERT_DELTA_CURR, secCurrItem.getCurrCode(), ALERT_DELTA_HIGH));
			secCurrOutDifference = checkDeltaCoeff(
					secCurrItem.getAvgStatOutLastHourDemand() + secCurrItem.getAvgStatRecOutCurrLastHourDemand(),
					secCurrItem.getAvgStatOutLastThreeHoursDemand()
							+ secCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand(),
					converter.convertValue(ALERT_DELTA_CURR, secCurrItem.getCurrCode(), ALERT_DELTA_LOW),
					converter.convertValue(ALERT_DELTA_CURR, secCurrItem.getCurrCode(), ALERT_DELTA_HIGH));
		}
		if (sec2CurrItem != null) {
			sec2CurrInDifference = checkDeltaCoeff(sec2CurrItem.getAvgStatRecInCurrLastHourDemand(),
					sec2CurrItem.getAvgStatRecInCurrLastThreeHoursDemand(),
					converter.convertValue(ALERT_DELTA_CURR, sec2CurrItem.getCurrCode(), ALERT_DELTA_LOW),
					converter.convertValue(ALERT_DELTA_CURR, sec2CurrItem.getCurrCode(), ALERT_DELTA_HIGH));
			sec2CurrOutDifference = checkDeltaCoeff(
					sec2CurrItem.getAvgStatOutLastHourDemand() + sec2CurrItem.getAvgStatRecOutCurrLastHourDemand(),
					sec2CurrItem.getAvgStatOutLastThreeHoursDemand()
							+ sec2CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand(),
					converter.convertValue(ALERT_DELTA_CURR, sec2CurrItem.getCurrCode(), ALERT_DELTA_LOW),
					converter.convertValue(ALERT_DELTA_CURR, sec2CurrItem.getCurrCode(), ALERT_DELTA_HIGH));
		}

		if (sec3CurrItem != null) {
			sec3CurrInDifference = checkDeltaCoeff(sec3CurrItem.getAvgStatRecInCurrLastHourDemand(),
					sec3CurrItem.getAvgStatRecInCurrLastThreeHoursDemand(),
					converter.convertValue(ALERT_DELTA_CURR, sec3CurrItem.getCurrCode(), ALERT_DELTA_LOW),
					converter.convertValue(ALERT_DELTA_CURR, sec3CurrItem.getCurrCode(), ALERT_DELTA_HIGH));
			sec3CurrOutDifference = checkDeltaCoeff(
					sec3CurrItem.getAvgStatOutLastHourDemand() + sec3CurrItem.getAvgStatRecOutCurrLastHourDemand(),
					sec3CurrItem.getAvgStatOutLastThreeHoursDemand()
							+ sec3CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand(),
					converter.convertValue(ALERT_DELTA_CURR, sec3CurrItem.getCurrCode(), ALERT_DELTA_LOW),
					converter.convertValue(ALERT_DELTA_CURR, sec3CurrItem.getCurrCode(), ALERT_DELTA_HIGH));
		}

		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmActualStateMapper mapper = session.getMapper(getMapperClass());
			check = mapper.getVCheck(forecast.getAtmId());
			if (check == 0) {
				Timestamp cashInStatDate = null, outOfCashOutDate = null, outOfCashInDate = null;
				Integer cashInStatValue = null, outOfCashOutCurr = null;
				if (cashInStat != null && cashInStat.getKey() != null)
					cashInStatDate = new Timestamp(cashInStat.getKey().getTime());

				if (cashInStat != null && cashInStat.getValue() != null)
					cashInStatValue = cashInStat.getValue();

				if (forecast.getOutOfCashOutDate() != null) {
					outOfCashOutDate = new Timestamp(
							CmUtils.truncateDateToHours(forecast.getOutOfCashOutDate()).getTime());
					outOfCashOutCurr = forecast.getOutOfCashOutCurr();
				}

				if (forecast.getOutOfCashInDate() != null)
					outOfCashInDate = new Timestamp(
							CmUtils.truncateDateToHours(forecast.getOutOfCashInDate()).getTime());

				mapper.insertAtmActualStateItem(forecast.getAtmId(), new Timestamp(cashOutStat.getKey().getTime()),
						cashOutStat.getValue(), cashInStatDate, cashInStatValue, new Timestamp(new Date().getTime()),
						outOfCashOutDate, outOfCashOutCurr, forecast.getOutOfCashOutResp(), outOfCashInDate,
						forecast.getOutOfCashInResp(),
						getCashOutHoursFromLastWithdrawal(sessionHolder, forecast.getAtmId()),
						getCashInHoursFromLastAddition(sessionHolder, forecast.getAtmId()),
						forecast.isNeedCurrRemainingAlert());

				Double mainCurr1 = null, mainCurr2 = null, mainCurr3 = null, mainCurr4 = null, mainCurr5 = null,
						mainCurr6 = null, secCurr1 = null, secCurr2 = null, secCurr3 = null, secCurr4 = null,
						secCurr5 = null, secCurr6 = null, sec2Curr1 = null, sec2Curr2 = null, sec2Curr3 = null,
						sec2Curr4 = null, sec2Curr5 = null, sec2Curr6 = null, sec3Curr1 = null, sec3Curr2 = null,
						sec3Curr3 = null, sec3Curr4 = null, sec3Curr5 = null, sec3Curr6 = null;

				if (mainCurrItem != null) {
					mainCurr1 = new Double(mainCurrItem.getAvgStatRecInCurrLastHourDemand());
					mainCurr2 = new Double(mainCurrItem.getAvgStatOutLastHourDemand()
							+ mainCurrItem.getAvgStatRecOutCurrLastHourDemand());
					mainCurr3 = new Double(mainCurrInDifference);
					mainCurr4 = new Double(mainCurrOutDifference);
					mainCurr5 = new Double(mainCurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					mainCurr6 = new Double(mainCurrItem.getAvgStatOutLastThreeHoursDemand()
							+ mainCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				}

				if (secCurrItem != null) {
					secCurr1 = new Double(secCurrItem.getAvgStatRecInCurrLastHourDemand());
					secCurr2 = new Double(secCurrItem.getAvgStatOutLastHourDemand()
							+ secCurrItem.getAvgStatRecOutCurrLastHourDemand());
					secCurr3 = new Double(secCurrInDifference);
					secCurr4 = new Double(secCurrOutDifference);
					secCurr5 = new Double(secCurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					secCurr6 = new Double(secCurrItem.getAvgStatOutLastThreeHoursDemand()
							+ secCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				}

				if (sec2CurrItem != null) {
					sec2Curr1 = new Double(sec2CurrItem.getAvgStatRecInCurrLastHourDemand());
					sec2Curr2 = new Double(sec2CurrItem.getAvgStatOutLastHourDemand()
							+ sec2CurrItem.getAvgStatRecOutCurrLastHourDemand());
					sec2Curr3 = new Double(sec2CurrInDifference);
					sec2Curr4 = new Double(sec2CurrOutDifference);
					sec2Curr5 = new Double(sec2CurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					sec2Curr6 = new Double(sec2CurrItem.getAvgStatOutLastThreeHoursDemand()
							+ sec2CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				}

				if (sec3CurrItem != null) {
					sec3Curr1 = new Double(sec3CurrItem.getAvgStatRecInCurrLastHourDemand());
					sec3Curr2 = new Double(sec3CurrItem.getAvgStatOutLastHourDemand()
							+ sec3CurrItem.getAvgStatRecOutCurrLastHourDemand());
					sec3Curr3 = new Double(sec3CurrInDifference);
					sec3Curr4 = new Double(sec3CurrOutDifference);
					sec3Curr5 = new Double(sec3CurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					sec3Curr6 = new Double(sec3CurrItem.getAvgStatOutLastThreeHoursDemand()
							+ sec3CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				}

				mapper.insertAtmAvgDemand(forecast.getAtmId(), mainCurr1, mainCurr2, mainCurr3, mainCurr4, mainCurr5,
						mainCurr6, secCurr1, secCurr2, secCurr3, secCurr4, secCurr5, secCurr6, sec2Curr1, sec2Curr2,
						sec2Curr3, sec2Curr4, sec2Curr5, sec2Curr6, sec3Curr1, sec3Curr2, sec3Curr3, sec3Curr4,
						sec3Curr5, sec3Curr6);
			} else {
				Timestamp cashInStatDate = null, outOfCashOutDate = null, outOfCashInDate = null;
				Integer cashInStatValue = null, outOfCashOutCurr = null;
				if (cashInStat != null && cashInStat.getKey() != null)
					cashInStatDate = new Timestamp(cashInStat.getKey().getTime());

				if (cashInStat != null && cashInStat.getValue() != null)
					cashInStatValue = cashInStat.getValue();

				if (forecast.getOutOfCashOutDate() != null) {
					outOfCashOutDate = new Timestamp(
							CmUtils.truncateDateToHours(forecast.getOutOfCashOutDate()).getTime());
					outOfCashOutCurr = forecast.getOutOfCashOutCurr();
				}

				if (forecast.getOutOfCashInDate() != null)
					outOfCashInDate = new Timestamp(
							CmUtils.truncateDateToHours(forecast.getOutOfCashInDate()).getTime());

				mapper.updateAtmActualStateItem(new Timestamp(cashOutStat.getKey().getTime()), cashOutStat.getValue(),
						cashInStatDate, cashInStatValue, outOfCashOutDate, outOfCashOutCurr,
						forecast.getOutOfCashOutResp(), outOfCashInDate, forecast.getOutOfCashInResp(),
						getCashOutHoursFromLastWithdrawal(sessionHolder, forecast.getAtmId()),
						getCashOutHoursFromLastWithdrawal(sessionHolder, forecast.getAtmId()),
						forecast.isNeedCurrRemainingAlert(), forecast.getAtmId());

				Double mainCurr1 = null, mainCurr2 = null, mainCurr3 = null, mainCurr4 = null, mainCurr5 = null,
						mainCurr6 = null, secCurr1 = null, secCurr2 = null, secCurr3 = null, secCurr4 = null,
						secCurr5 = null, secCurr6 = null, sec2Curr1 = null, sec2Curr2 = null, sec2Curr3 = null,
						sec2Curr4 = null, sec2Curr5 = null, sec2Curr6 = null, sec3Curr1 = null, sec3Curr2 = null,
						sec3Curr3 = null, sec3Curr4 = null, sec3Curr5 = null, sec3Curr6 = null;

				if (mainCurrItem != null) {
					mainCurr1 = new Double(mainCurrItem.getAvgStatRecInCurrLastHourDemand());
					mainCurr2 = new Double(mainCurrItem.getAvgStatOutLastHourDemand()
							+ mainCurrItem.getAvgStatRecOutCurrLastHourDemand());
					mainCurr3 = new Double(mainCurrInDifference);
					mainCurr4 = new Double(mainCurrOutDifference);
					mainCurr5 = new Double(mainCurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					mainCurr6 = new Double(mainCurrItem.getAvgStatOutLastThreeHoursDemand()
							+ mainCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				}

				if (secCurrItem != null) {
					secCurr1 = new Double(secCurrItem.getAvgStatRecInCurrLastHourDemand());
					secCurr2 = new Double(secCurrItem.getAvgStatOutLastHourDemand()
							+ secCurrItem.getAvgStatRecOutCurrLastHourDemand());
					secCurr3 = new Double(secCurrInDifference);
					secCurr4 = new Double(secCurrOutDifference);
					secCurr5 = new Double(secCurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					secCurr6 = new Double(secCurrItem.getAvgStatOutLastThreeHoursDemand()
							+ secCurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				}

				if (sec2CurrItem != null) {
					sec2Curr1 = new Double(sec2CurrItem.getAvgStatRecInCurrLastHourDemand());
					sec2Curr2 = new Double(sec2CurrItem.getAvgStatOutLastHourDemand()
							+ sec2CurrItem.getAvgStatRecOutCurrLastHourDemand());
					sec2Curr3 = new Double(sec2CurrInDifference);
					sec2Curr4 = new Double(sec2CurrOutDifference);
					sec2Curr5 = new Double(sec2CurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					sec2Curr6 = new Double(sec2CurrItem.getAvgStatOutLastThreeHoursDemand()
							+ sec2CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				}

				if (sec3CurrItem != null) {
					sec3Curr1 = new Double(sec3CurrItem.getAvgStatRecInCurrLastHourDemand());
					sec3Curr2 = new Double(sec3CurrItem.getAvgStatOutLastHourDemand()
							+ sec3CurrItem.getAvgStatRecOutCurrLastHourDemand());
					sec3Curr3 = new Double(sec3CurrInDifference);
					sec3Curr4 = new Double(sec3CurrOutDifference);
					sec3Curr5 = new Double(sec3CurrItem.getAvgStatRecInCurrLastThreeHoursDemand());
					sec3Curr6 = new Double(sec3CurrItem.getAvgStatOutLastThreeHoursDemand()
							+ sec3CurrItem.getAvgStatRecOutCurrLastThreeHoursDemand());
				}

				mapper.updateAtmAvgDemand(mainCurr1, mainCurr2, mainCurr3, mainCurr4, mainCurr5, mainCurr6, secCurr1,
						secCurr2, secCurr3, secCurr4, secCurr5, secCurr6, sec2Curr1, sec2Curr2, sec2Curr3, sec2Curr4,
						sec2Curr5, sec2Curr6, sec3Curr1, sec3Curr2, sec3Curr3, sec3Curr4, sec3Curr5, sec3Curr6,
						forecast.getAtmId());

				if (updateLastUpdateDate)
					mapper.updateAtmActualState(new Timestamp(new Date().getTime()), forecast.getAtmId());
			}
		} finally {
			// session.commit();
			session.close();
		}
	}

	protected static void updateInitialsForAtm(ISessionHolder sessionHolder, int atmId, int cashInVolume,
			int rejectVolume, int cashInRVolume) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			session.getMapper(getMapperClass()).updateInitialsForAtm(cashInVolume, rejectVolume, cashInRVolume, atmId);
		} finally {
			// session.commit();
			session.close();
		}
	}

	public static boolean checkAtmActStateTable(ISessionHolder sessionHolder) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		int cnt = 0;
		try {
			cnt = session.getMapper(getMapperClass()).checkAtmActStateTable();
		} finally {
			session.close();
		}
		return cnt > 0;
	}

	protected static void addCashOutCassettes(ISessionHolder sessionHolder, int atmId, int ecnashmentId,
			List<AtmCassetteItem> cassList) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			List<AtmCassetteItem> cassettes = session.getMapper(getMapperClass()).getCashOutCassettes(ecnashmentId,
					atmId, new AtmCassetteOutResultHandler());
			for (AtmCassetteItem cassette : cassettes)
				cassette.setType(AtmCassetteType.CASH_OUT_CASS);
			cassList.addAll(cassettes);
		} finally {
			session.close();
		}
	}

	protected static void addCashInRecyclingCassettes(ISessionHolder sessionHolder, int atmId, int cashInEcnashmentId,
			List<AtmCassetteItem> cassList) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			List<AtmCassetteItem> cassettes = session.getMapper(getMapperClass())
					.getCashInRecyclingCassettes(cashInEcnashmentId, atmId, new AtmCassetteInResultHandler());
			for (AtmCassetteItem cassette : cassettes)
				cassette.setType(AtmCassetteType.CASH_IN_R_CASS);
			cassList.addAll(cassettes);
		} finally {
			session.close();
		}
	}

	public static void saveAtmCassettes(ISessionHolder sessionHolder, int atmId, List<AtmCassetteItem> atmCassList) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmActualStateMapper mapper = session.getMapper(getMapperClass());
			int check = 0;

			for (AtmCassetteItem cass : atmCassList) {

				check = mapper.checkAtmCassettes(atmId, cass.getType().getId(), cass.getNumber());
				if (check == 0)
					mapper.insertAtmCassettes(atmId, cass.getType().getId(), cass.getNumber(), cass.getCurr(),
							cass.getDenom());
				else
					mapper.updateAtmCassettes(atmId, cass.getType().getId(), cass.getNumber(), cass.getCurr(),
							cass.getDenom());
			}
			mapper.deleteAtmCassettes(atmId);
		} finally {
			// session.commit();
			session.close();
		}
	}

	protected static void updateCalculatedRemainingForAtms(ISessionHolder sessionHolder) throws SQLException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		try {
			AtmActualStateMapper mapper = session.getMapper(getMapperClass());
			mapper.updateCalculatedRemainingForAtms1(AtmCassetteType.CASH_OUT_CASS.getId());
			mapper.updateCalculatedRemainingForAtms2(AtmCassetteType.CASH_IN_CASS.getId());
			mapper.updateCalculatedRemainingForAtms3(AtmCassetteType.CASH_IN_R_CASS.getId());
		} finally {
			// session.commit();
			session.close();
		}
	}

	public static void checkLoadedBalances(ISessionHolder sessionHolder) throws MonitoringException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		boolean balancesAreIncorrect = false;
		try {
			AtmActualStateMapper mapper = session.getMapper(getMapperClass());
			List<BalanceItem> balances = mapper.checkLoadedBalances1(BALANCES_CHECK_THRESHHOLD);

			for (BalanceItem item : balances) {
				balancesAreIncorrect = true;
				logger.error(BALANCES_CHECK_ERROR_FORMAT, item.getAtmId(), item.getCassType(), item.getCassNumber(),
						item.getRemainingLoad(), item.getRemainingCalc());
			}
			if (balancesAreIncorrect) {
				throw new MonitoringException(
						"Loaded balances are incorrect, should be logged in CASH_MANAGEMENT logger");
			}
		} finally {
			session.close();
		}
	}

	public static void checkLoadedBalances(ISessionHolder sessionHolder, List<Integer> atmList)
			throws MonitoringException {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		boolean balancesAreIncorrect = false;
		try {
			AtmActualStateMapper mapper = session.getMapper(getMapperClass());
			List<BalanceItem> balances = mapper.checkLoadedBalances2(BALANCES_CHECK_THRESHHOLD, atmList);

			for (BalanceItem item : balances) {
				balancesAreIncorrect = true;
				logger.error(BALANCES_CHECK_ERROR_FORMAT, item.getAtmId(), item.getCassType(), item.getCassNumber(),
						item.getRemainingLoad(), item.getRemainingCalc());
			}
			if (balancesAreIncorrect) {
				throw new MonitoringException(
						"Loaded balances are incorrect, should be logged in CASH_MANAGEMENT logger");
			}
		} finally {
			session.close();
		}
	}

	public static boolean getAtmDeviceState(ISessionHolder sessionHolder, int atmId) {
		SqlSession session = sessionHolder.getSession(getMapperClass());
		Integer atmState = null;
		try {
			atmState = session.getMapper(getMapperClass()).getAtmDeviceState(atmId,
					ORMUtils.getSingleRowExpression(session));
			if (atmState != null)
				return atmState == 0;
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			session.close();
		}
		return false;
	}
}
